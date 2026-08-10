from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from proxima_traffic import config
from proxima_traffic.config import Settings
from proxima_traffic.db import Database


@pytest.mark.parametrize(
    ("platform_name", "environment_name", "directory_name"),
    [
        ("win32", "LOCALAPPDATA", "Proxima Traffic"),
        ("linux", "XDG_DATA_HOME", "proxima-traffic"),
    ],
)
def test_default_data_paths_use_a_stable_system_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    platform_name: str,
    environment_name: str,
    directory_name: str,
) -> None:
    monkeypatch.setattr(config.sys, "platform", platform_name)
    monkeypatch.setenv(environment_name, str(tmp_path))
    monkeypatch.delenv("PROXIMA_DATABASE_URL", raising=False)
    monkeypatch.delenv("PROXIMA_MEDIA_STORAGE_PATH", raising=False)
    monkeypatch.delenv("PROXIMA_BACKUP_STORAGE_PATH", raising=False)

    settings = Settings(_env_file=None, worker_enabled=False)
    data_directory = tmp_path / directory_name

    expected_database_url = f"sqlite+aiosqlite:///{(data_directory / 'proxima.db').as_posix()}"
    assert settings.database_url == expected_database_url
    assert settings.media_storage_path == str(data_directory / "media")
    assert settings.backup_storage_path == str(data_directory / "backups")


async def test_database_creates_parent_for_an_absolute_sqlite_path(tmp_path: Path) -> None:
    database_path = tmp_path / "nested" / "proxima.db"
    database = Database(f"sqlite+aiosqlite:///{database_path}")

    await database.create_schema()
    await database.dispose()

    assert database_path.is_file()


def test_telegram_api_credentials_are_validated_as_a_pair() -> None:
    settings = Settings(
        telegram_api_id=12345,
        telegram_api_hash="0123456789abcdef0123456789abcdef",
        worker_enabled=False,
    )
    assert settings.telegram_api == (12345, "0123456789abcdef0123456789abcdef")

    with pytest.raises(ValidationError, match="must be set together"):
        Settings(telegram_api_id=12345, telegram_api_hash=None, worker_enabled=False)

    with pytest.raises(ValidationError, match="positive integer"):
        Settings(
            telegram_api_id=-1,
            telegram_api_hash="0123456789abcdef0123456789abcdef",
            worker_enabled=False,
        )

    with pytest.raises(ValidationError, match="32 hexadecimal"):
        Settings(telegram_api_id=12345, telegram_api_hash="not-a-valid-hash", worker_enabled=False)


def test_channel_comment_settings_have_bounded_safe_defaults() -> None:
    settings = Settings(worker_enabled=False)
    assert settings.channel_comments_enabled is True
    assert settings.channel_comments_poll_seconds == 5
    assert settings.channel_comments_scan_interval_seconds == 60
    assert settings.channel_comments_max_post_age_seconds == 3600
    assert settings.channel_comments_scan_limit == 30

    with pytest.raises(ValidationError, match="SCAN_INTERVAL"):
        Settings(worker_enabled=False, channel_comments_scan_interval_seconds=14)
    with pytest.raises(ValidationError, match="MAX_POST_AGE"):
        Settings(worker_enabled=False, channel_comments_max_post_age_seconds=59)
    with pytest.raises(ValidationError, match="SCAN_LIMIT"):
        Settings(worker_enabled=False, channel_comments_scan_limit=101)


def test_typing_settings_have_short_safe_defaults() -> None:
    settings = Settings(worker_enabled=False)
    assert settings.account_typing_enabled is True
    assert settings.account_typing_min_seconds == 2
    assert settings.account_typing_max_seconds == 4

    with pytest.raises(ValidationError, match="TYPING_MAX_SECONDS"):
        Settings(
            worker_enabled=False,
            account_typing_min_seconds=4,
            account_typing_max_seconds=2,
        )
    with pytest.raises(ValidationError, match="must not exceed 15"):
        Settings(worker_enabled=False, account_typing_max_seconds=16)


def test_membership_gate_retry_settings_are_bounded() -> None:
    settings = Settings(worker_enabled=False)
    assert settings.membership_gate_recovery_enabled is True
    assert settings.membership_gate_retry_delay_seconds == 300
    assert settings.membership_gate_max_channels == 8
    assert settings.membership_gate_scan_limit == 50

    with pytest.raises(ValidationError, match="MEMBERSHIP_GATE_RETRY_DELAY"):
        Settings(worker_enabled=False, membership_gate_retry_delay_seconds=59)
    with pytest.raises(ValidationError, match="MEMBERSHIP_GATE_MAX_CHANNELS"):
        Settings(worker_enabled=False, membership_gate_max_channels=21)
    with pytest.raises(ValidationError, match="MEMBERSHIP_GATE_SCAN_LIMIT"):
        Settings(worker_enabled=False, membership_gate_scan_limit=9)


def test_live_workers_require_dedicated_non_default_secrets() -> None:
    with pytest.raises(ValidationError, match="ADMIN_API_KEY"):
        Settings(
            dry_run=False,
            live_execution_armed=True,
            worker_enabled=True,
            proxy_encryption_key="p" * 16,
            account_encryption_key="a" * 16,
        )

    settings = Settings(
        dry_run=False,
        live_execution_armed=True,
        worker_enabled=True,
        admin_api_key="k" * 16,
        proxy_encryption_key="p" * 16,
        account_encryption_key="a" * 16,
    )
    assert settings.account_require_proxy is True


def test_production_environment_is_normalized_before_safety_checks() -> None:
    with pytest.raises(ValidationError, match="AUTO_CREATE_SCHEMA"):
        Settings(
            environment=" production ",
            dry_run=True,
            worker_enabled=False,
            admin_api_key="k" * 16,
            proxy_encryption_key="p" * 16,
        )


def test_worker_retry_base_must_be_positive() -> None:
    with pytest.raises(ValidationError, match="RETRY_BASE"):
        Settings(worker_enabled=False, worker_retry_base_seconds=0)
