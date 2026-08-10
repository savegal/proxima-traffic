import os
import sys
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def default_data_directory() -> Path:
    """Return a stable per-user application-data directory on every supported OS."""
    if sys.platform == "win32":
        root = os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local"
        return Path(root) / "Proxima Traffic"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "Proxima Traffic"
    root = os.environ.get("XDG_DATA_HOME")
    if root:
        return Path(root) / "proxima-traffic"
    return Path.home() / ".local" / "share" / "proxima-traffic"


def default_database_url() -> str:
    return f"sqlite+aiosqlite:///{(default_data_directory() / 'proxima.db').as_posix()}"


def default_media_storage_path() -> str:
    return str(default_data_directory() / "media")


def default_backup_storage_path() -> str:
    return str(default_data_directory() / "backups")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="PROXIMA_",
        extra="ignore",
    )

    app_name: str = "Proxima Traffic"
    environment: str = "development"
    database_url: str = Field(default_factory=default_database_url)
    timezone: str = "Asia/Jerusalem"
    admin_api_key: SecretStr = SecretStr("local-development-only-change-me")
    bot_token: SecretStr | None = None
    bot_admin_ids: str = ""
    bot_log_chat_id: int | None = None
    proxy_encryption_key: SecretStr | None = None
    proxy_check_url: str = "https://api.ipify.org?format=json"
    proxy_check_timeout_seconds: float = 12.0
    proxy_quarantine_after_failures: int = 3
    account_encryption_key: SecretStr | None = None
    telegram_api_id: int | None = None
    telegram_api_hash: SecretStr | None = None
    tdata_two_factor_password: SecretStr | None = None
    account_require_proxy: bool = True
    account_connect_timeout_seconds: float = 30.0
    account_flood_sleep_threshold: int = 60
    account_delivery_verify_enabled: bool = True
    account_delivery_verify_delay_seconds: float = 1.0
    account_delivery_verify_confirm_delay_seconds: float = 1.5
    account_delivery_verify_timeout_seconds: float = 5.0
    membership_gate_recovery_enabled: bool = True
    membership_gate_retry_delay_seconds: int = 300
    membership_gate_max_channels: int = 8
    membership_gate_scan_limit: int = 50
    delayed_delivery_verify_enabled: bool = True
    delayed_delivery_verify_delay_seconds: int = 600
    delayed_delivery_verify_poll_seconds: float = 30.0
    delayed_delivery_verify_batch_size: int = 20
    delayed_delivery_verify_retry_seconds: int = 300
    delayed_delivery_verify_timeout_seconds: float = 30.0
    account_pace_min_seconds: float = 0.0
    account_pace_max_seconds: float = 0.0
    account_typing_enabled: bool = True
    account_typing_min_seconds: float = 2.0
    account_typing_max_seconds: float = 4.0
    account_auto_join: bool = False
    account_daily_limit: int = 200
    account_micro_break_every: int = 0
    account_micro_break_min_seconds: float = 300.0
    account_micro_break_max_seconds: float = 900.0
    account_join_daily_limit: int = 0
    account_join_min_interval_seconds: float = 0.0
    flood_buffer_multiplier: float = 1.2
    flood_escalation_threshold: int = 3
    flood_window_hours: float = 12.0
    flood_cooldown_hours: float = 24.0
    account_restriction_cooldown_hours: float = 24.0
    max_flood_wait_seconds: float = 86400.0
    spambot_enabled: bool = True
    spambot_timeout_seconds: float = 15.0
    spambot_min_check_interval_seconds: int = 21600
    spambot_unknown_cooldown_seconds: int = 7200
    spambot_maintenance_interval_seconds: int = 86400
    spambot_maintenance_batch_size: int = 20
    media_storage_path: str = Field(default_factory=default_media_storage_path)
    backup_storage_path: str = Field(default_factory=default_backup_storage_path)
    media_max_bytes: int = 50 * 1024 * 1024
    preflight_required_live: bool = True
    preflight_max_age_seconds: int = 3600
    dry_run: bool = True
    live_execution_armed: bool = False
    auto_create_schema: bool = True
    log_level: str = "INFO"
    worker_enabled: bool = True
    api_worker_enabled: bool = False
    worker_poll_seconds: float = 1.0
    worker_lease_seconds: int = 30
    worker_heartbeat_seconds: float = 5.0
    worker_step_timeout_seconds: float = 120.0
    worker_readiness_max_stale_seconds: float = 300.0
    worker_concurrency: int = 4
    shutdown_grace_seconds: float = 20.0
    worker_max_attempts: int = 5
    worker_retry_base_seconds: int = 2
    worker_retry_max_seconds: int = 300
    channel_comments_enabled: bool = True
    channel_comments_poll_seconds: float = 5.0
    channel_comments_scan_interval_seconds: int = 60
    channel_comments_max_post_age_seconds: int = 3600
    channel_comments_scan_limit: int = 30
    channel_comments_scan_timeout_seconds: float = 30.0
    channel_comments_scan_concurrency: int = 4
    channel_comments_mention_cooldown_days: int = 30
    participant_reactions_enabled: bool = True
    participant_reactions_poll_seconds: float = 5.0
    participant_reactions_scan_interval_seconds: int = 60
    participant_reactions_scan_limit: int = 100
    participant_reactions_max_message_age_seconds: int = 3600
    participant_reactions_sender_cooldown_seconds: int = 3600
    direct_messages_enabled: bool = True
    direct_messages_poll_seconds: float = 2.0
    direct_messages_worker_concurrency: int = 2
    direct_messages_upload_max_bytes: int = 20 * 1024 * 1024
    subscriber_invites_enabled: bool = True
    subscriber_invites_poll_seconds: float = 2.0
    subscriber_invites_worker_concurrency: int = 2

    @property
    def allowed_bot_admin_ids(self) -> frozenset[int]:
        values: set[int] = set()
        for raw_value in self.bot_admin_ids.split(","):
            value = raw_value.strip()
            if not value:
                continue
            try:
                parsed = int(value)
            except ValueError as exc:
                raise ValueError("PROXIMA_BOT_ADMIN_IDS must contain numeric Telegram IDs") from exc
            if parsed <= 0:
                raise ValueError("PROXIMA_BOT_ADMIN_IDS must contain positive Telegram IDs")
            values.add(parsed)
        return frozenset(values)

    def require_control_bot(self) -> tuple[str, frozenset[int]]:
        token = self.bot_token.get_secret_value().strip() if self.bot_token else ""
        admin_ids = self.allowed_bot_admin_ids
        if not token:
            raise ValueError("PROXIMA_BOT_TOKEN is required to start the control bot")
        if not admin_ids:
            raise ValueError("PROXIMA_BOT_ADMIN_IDS must contain at least one Telegram user ID")
        return token, admin_ids

    @property
    def account_encryption_secret(self) -> str:
        """Secret used to encrypt stored account sessions.

        Falls back to the proxy key and then the admin key so the software works
        out of the box, while allowing a dedicated key in production.
        """
        if self.account_encryption_key is not None:
            return self.account_encryption_key.get_secret_value()
        if self.proxy_encryption_key is not None:
            return self.proxy_encryption_key.get_secret_value()
        return self.admin_api_key.get_secret_value()

    @property
    def telegram_api(self) -> tuple[int, str] | None:
        """Own Telegram application credentials used for imported accounts."""
        if self.telegram_api_id is None or self.telegram_api_hash is None:
            return None
        secret = self.telegram_api_hash.get_secret_value().strip()
        if not secret:
            return None
        return self.telegram_api_id, secret

    @model_validator(mode="after")
    def reject_unsafe_production_defaults(self) -> "Settings":
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("PROXIMA_TIMEZONE must be a valid IANA timezone") from exc
        telegram_api_hash = (
            self.telegram_api_hash.get_secret_value().strip()
            if self.telegram_api_hash is not None
            else ""
        )
        if (self.telegram_api_id is None) != (not telegram_api_hash):
            raise ValueError(
                "PROXIMA_TELEGRAM_API_ID and PROXIMA_TELEGRAM_API_HASH must be set together"
            )
        if self.telegram_api_id is not None:
            if self.telegram_api_id <= 0:
                raise ValueError("PROXIMA_TELEGRAM_API_ID must be a positive integer")
            if len(telegram_api_hash) != 32 or any(
                character not in "0123456789abcdefABCDEF" for character in telegram_api_hash
            ):
                raise ValueError(
                    "PROXIMA_TELEGRAM_API_HASH must contain exactly 32 hexadecimal characters"
                )
        environment = self.environment.strip().casefold()
        if not environment:
            raise ValueError("PROXIMA_ENVIRONMENT must not be blank")
        live_workers_enabled = not self.dry_run and (self.worker_enabled or self.api_worker_enabled)
        if environment == "production":
            if len(self.admin_api_key.get_secret_value()) < 16 or (
                self.admin_api_key.get_secret_value() == "local-development-only-change-me"
            ):
                raise ValueError("PROXIMA_ADMIN_API_KEY must be a non-default secret in production")
            if self.auto_create_schema:
                raise ValueError("PROXIMA_AUTO_CREATE_SCHEMA must be false in production")
            if (
                self.proxy_encryption_key is None
                or len(self.proxy_encryption_key.get_secret_value()) < 16
            ):
                raise ValueError("PROXIMA_PROXY_ENCRYPTION_KEY must be set in production")
        if live_workers_enabled:
            if len(self.admin_api_key.get_secret_value()) < 16 or (
                self.admin_api_key.get_secret_value() == "local-development-only-change-me"
            ):
                raise ValueError(
                    "PROXIMA_ADMIN_API_KEY must be a non-default secret for live execution"
                )
            if (
                self.proxy_encryption_key is None
                or len(self.proxy_encryption_key.get_secret_value()) < 16
            ):
                raise ValueError("PROXIMA_PROXY_ENCRYPTION_KEY must be set for live execution")
            if (
                self.account_encryption_key is None
                or len(self.account_encryption_key.get_secret_value()) < 16
            ):
                raise ValueError("PROXIMA_ACCOUNT_ENCRYPTION_KEY must be set for live execution")
        if live_workers_enabled and not self.live_execution_armed:
            raise ValueError(
                "live worker is not armed; set PROXIMA_LIVE_EXECUTION_ARMED=true only after "
                "reviewing the database, accounts, targets and queued launches"
            )
        if self.account_connect_timeout_seconds <= 0:
            raise ValueError("PROXIMA_ACCOUNT_CONNECT_TIMEOUT_SECONDS must be positive")
        if self.account_delivery_verify_delay_seconds < 0:
            raise ValueError("PROXIMA_ACCOUNT_DELIVERY_VERIFY_DELAY_SECONDS must not be negative")
        if self.account_delivery_verify_confirm_delay_seconds < 0:
            raise ValueError(
                "PROXIMA_ACCOUNT_DELIVERY_VERIFY_CONFIRM_DELAY_SECONDS must not be negative"
            )
        if self.account_delivery_verify_timeout_seconds <= 0:
            raise ValueError("PROXIMA_ACCOUNT_DELIVERY_VERIFY_TIMEOUT_SECONDS must be positive")
        if not 60 <= self.membership_gate_retry_delay_seconds <= 3600:
            raise ValueError(
                "PROXIMA_MEMBERSHIP_GATE_RETRY_DELAY_SECONDS must be between 60 and 3600"
            )
        if not 1 <= self.membership_gate_max_channels <= 20:
            raise ValueError("PROXIMA_MEMBERSHIP_GATE_MAX_CHANNELS must be between 1 and 20")
        if not 10 <= self.membership_gate_scan_limit <= 100:
            raise ValueError("PROXIMA_MEMBERSHIP_GATE_SCAN_LIMIT must be between 10 and 100")
        if not 60 <= self.delayed_delivery_verify_delay_seconds <= 24 * 3600:
            raise ValueError(
                "PROXIMA_DELAYED_DELIVERY_VERIFY_DELAY_SECONDS must be between 60 and 86400"
            )
        if not 5 <= self.delayed_delivery_verify_poll_seconds <= 3600:
            raise ValueError(
                "PROXIMA_DELAYED_DELIVERY_VERIFY_POLL_SECONDS must be between 5 and 3600"
            )
        if not 1 <= self.delayed_delivery_verify_batch_size <= 100:
            raise ValueError("PROXIMA_DELAYED_DELIVERY_VERIFY_BATCH_SIZE must be between 1 and 100")
        if not 60 <= self.delayed_delivery_verify_retry_seconds <= 3600:
            raise ValueError(
                "PROXIMA_DELAYED_DELIVERY_VERIFY_RETRY_SECONDS must be between 60 and 3600"
            )
        if not 1 <= self.delayed_delivery_verify_timeout_seconds <= 300:
            raise ValueError(
                "PROXIMA_DELAYED_DELIVERY_VERIFY_TIMEOUT_SECONDS must be between 1 and 300"
            )
        if self.account_pace_min_seconds < 0 or self.account_pace_max_seconds < 0:
            raise ValueError("PROXIMA_ACCOUNT_PACE_*_SECONDS must not be negative")
        if self.account_pace_max_seconds < self.account_pace_min_seconds:
            raise ValueError(
                "PROXIMA_ACCOUNT_PACE_MAX_SECONDS must be >= PROXIMA_ACCOUNT_PACE_MIN_SECONDS"
            )
        if self.account_typing_min_seconds < 0 or self.account_typing_max_seconds < 0:
            raise ValueError("PROXIMA_ACCOUNT_TYPING_*_SECONDS must not be negative")
        if self.account_typing_max_seconds < self.account_typing_min_seconds:
            raise ValueError(
                "PROXIMA_ACCOUNT_TYPING_MAX_SECONDS must be >= PROXIMA_ACCOUNT_TYPING_MIN_SECONDS"
            )
        if self.account_typing_max_seconds > 15:
            raise ValueError("PROXIMA_ACCOUNT_TYPING_MAX_SECONDS must not exceed 15")
        if self.account_daily_limit < 0:
            raise ValueError("PROXIMA_ACCOUNT_DAILY_LIMIT must not be negative")
        if self.account_micro_break_every < 0:
            raise ValueError("PROXIMA_ACCOUNT_MICRO_BREAK_EVERY must not be negative")
        if self.account_micro_break_min_seconds < 0 or self.account_micro_break_max_seconds < 0:
            raise ValueError("PROXIMA_ACCOUNT_MICRO_BREAK_*_SECONDS must not be negative")
        if self.account_micro_break_max_seconds < self.account_micro_break_min_seconds:
            raise ValueError(
                "PROXIMA_ACCOUNT_MICRO_BREAK_MAX_SECONDS must be >= "
                "PROXIMA_ACCOUNT_MICRO_BREAK_MIN_SECONDS"
            )
        if self.account_join_daily_limit < 0:
            raise ValueError("PROXIMA_ACCOUNT_JOIN_DAILY_LIMIT must not be negative")
        if self.account_join_min_interval_seconds < 0:
            raise ValueError("PROXIMA_ACCOUNT_JOIN_MIN_INTERVAL_SECONDS must not be negative")
        if self.flood_buffer_multiplier < 1:
            raise ValueError("PROXIMA_FLOOD_BUFFER_MULTIPLIER must be >= 1")
        if self.flood_escalation_threshold < 1:
            raise ValueError("PROXIMA_FLOOD_ESCALATION_THRESHOLD must be >= 1")
        if self.flood_window_hours <= 0 or self.flood_cooldown_hours <= 0:
            raise ValueError("PROXIMA_FLOOD_WINDOW_HOURS/COOLDOWN_HOURS must be positive")
        if self.account_restriction_cooldown_hours <= 0:
            raise ValueError("PROXIMA_ACCOUNT_RESTRICTION_COOLDOWN_HOURS must be positive")
        if self.max_flood_wait_seconds <= 0:
            raise ValueError("PROXIMA_MAX_FLOOD_WAIT_SECONDS must be positive")
        if self.spambot_timeout_seconds <= 0:
            raise ValueError("PROXIMA_SPAMBOT_TIMEOUT_SECONDS must be positive")
        if self.spambot_min_check_interval_seconds < 60:
            raise ValueError("PROXIMA_SPAMBOT_MIN_CHECK_INTERVAL_SECONDS must be at least 60")
        if self.spambot_unknown_cooldown_seconds < 60:
            raise ValueError("PROXIMA_SPAMBOT_UNKNOWN_COOLDOWN_SECONDS must be at least 60")
        if self.spambot_maintenance_interval_seconds < 3600:
            raise ValueError("PROXIMA_SPAMBOT_MAINTENANCE_INTERVAL_SECONDS must be at least 3600")
        if self.spambot_maintenance_batch_size < 1:
            raise ValueError("PROXIMA_SPAMBOT_MAINTENANCE_BATCH_SIZE must be positive")
        if self.media_max_bytes <= 0:
            raise ValueError("PROXIMA_MEDIA_MAX_BYTES must be positive")
        if self.preflight_max_age_seconds < 60:
            raise ValueError("PROXIMA_PREFLIGHT_MAX_AGE_SECONDS must be at least 60")
        if self.worker_poll_seconds <= 0:
            raise ValueError("PROXIMA_WORKER_POLL_SECONDS must be positive")
        if self.worker_lease_seconds < 5:
            raise ValueError("PROXIMA_WORKER_LEASE_SECONDS must be at least 5")
        if not 0 < self.worker_heartbeat_seconds < self.worker_lease_seconds:
            raise ValueError(
                "PROXIMA_WORKER_HEARTBEAT_SECONDS must be positive and below lease seconds"
            )
        if self.worker_step_timeout_seconds <= 0:
            raise ValueError("PROXIMA_WORKER_STEP_TIMEOUT_SECONDS must be positive")
        if self.worker_readiness_max_stale_seconds < 30:
            raise ValueError("PROXIMA_WORKER_READINESS_MAX_STALE_SECONDS must be at least 30")
        if not 1 <= self.shutdown_grace_seconds <= 300:
            raise ValueError("PROXIMA_SHUTDOWN_GRACE_SECONDS must be between 1 and 300")
        if not 1 <= self.worker_concurrency <= 32:
            raise ValueError("PROXIMA_WORKER_CONCURRENCY must be between 1 and 32")
        if self.direct_messages_poll_seconds <= 0:
            raise ValueError("PROXIMA_DIRECT_MESSAGES_POLL_SECONDS must be positive")
        if not 1 <= self.direct_messages_worker_concurrency <= 8:
            raise ValueError("PROXIMA_DIRECT_MESSAGES_WORKER_CONCURRENCY must be between 1 and 8")
        if self.subscriber_invites_poll_seconds <= 0:
            raise ValueError("PROXIMA_SUBSCRIBER_INVITES_POLL_SECONDS must be positive")
        if not 1 <= self.subscriber_invites_worker_concurrency <= 8:
            raise ValueError(
                "PROXIMA_SUBSCRIBER_INVITES_WORKER_CONCURRENCY must be between 1 and 8"
            )
        if not 1 * 1024 * 1024 <= self.direct_messages_upload_max_bytes <= 100 * 1024 * 1024:
            raise ValueError(
                "PROXIMA_DIRECT_MESSAGES_UPLOAD_MAX_BYTES must be between 1 and 100 MiB"
            )
        if self.worker_max_attempts < 1:
            raise ValueError("PROXIMA_WORKER_MAX_ATTEMPTS must be positive")
        if self.worker_retry_base_seconds < 1:
            raise ValueError("PROXIMA_WORKER_RETRY_BASE_SECONDS must be positive")
        if self.worker_retry_max_seconds < self.worker_retry_base_seconds:
            raise ValueError("PROXIMA_WORKER_RETRY_MAX_SECONDS must be at least retry base seconds")
        if self.proxy_check_timeout_seconds <= 0:
            raise ValueError("PROXIMA_PROXY_CHECK_TIMEOUT_SECONDS must be positive")
        if self.proxy_quarantine_after_failures < 1:
            raise ValueError("PROXIMA_PROXY_QUARANTINE_AFTER_FAILURES must be positive")
        if self.channel_comments_poll_seconds <= 0:
            raise ValueError("PROXIMA_CHANNEL_COMMENTS_POLL_SECONDS must be positive")
        if not 15 <= self.channel_comments_scan_interval_seconds <= 3600:
            raise ValueError(
                "PROXIMA_CHANNEL_COMMENTS_SCAN_INTERVAL_SECONDS must be between 15 and 3600"
            )
        if not 60 <= self.channel_comments_max_post_age_seconds <= 7 * 24 * 3600:
            raise ValueError(
                "PROXIMA_CHANNEL_COMMENTS_MAX_POST_AGE_SECONDS must be between 60 and 604800"
            )
        if not 1 <= self.channel_comments_scan_limit <= 100:
            raise ValueError("PROXIMA_CHANNEL_COMMENTS_SCAN_LIMIT must be between 1 and 100")
        if not 1 <= self.channel_comments_scan_timeout_seconds <= 120:
            raise ValueError(
                "PROXIMA_CHANNEL_COMMENTS_SCAN_TIMEOUT_SECONDS must be between 1 and 120"
            )
        if not 1 <= self.channel_comments_scan_concurrency <= 20:
            raise ValueError("PROXIMA_CHANNEL_COMMENTS_SCAN_CONCURRENCY must be between 1 and 20")
        if not 1 <= self.channel_comments_mention_cooldown_days <= 365:
            raise ValueError(
                "PROXIMA_CHANNEL_COMMENTS_MENTION_COOLDOWN_DAYS must be between 1 and 365"
            )
        if self.participant_reactions_poll_seconds <= 0:
            raise ValueError("PROXIMA_PARTICIPANT_REACTIONS_POLL_SECONDS must be positive")
        if not 15 <= self.participant_reactions_scan_interval_seconds <= 3600:
            raise ValueError(
                "PROXIMA_PARTICIPANT_REACTIONS_SCAN_INTERVAL_SECONDS must be between 15 and 3600"
            )
        if not 1 <= self.participant_reactions_scan_limit <= 100:
            raise ValueError("PROXIMA_PARTICIPANT_REACTIONS_SCAN_LIMIT must be between 1 and 100")
        if not 60 <= self.participant_reactions_max_message_age_seconds <= 7 * 24 * 3600:
            raise ValueError(
                "PROXIMA_PARTICIPANT_REACTIONS_MAX_MESSAGE_AGE_SECONDS must be "
                "between 60 and 604800"
            )
        if not 0 <= self.participant_reactions_sender_cooldown_seconds <= 30 * 24 * 3600:
            raise ValueError(
                "PROXIMA_PARTICIPANT_REACTIONS_SENDER_COOLDOWN_SECONDS must be "
                "between 0 and 2592000"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
