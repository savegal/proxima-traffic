from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import floor

from proxima_traffic.models import Account, ensure_utc


@dataclass(frozen=True, slots=True)
class BroadcastProfile:
    key: str
    title: str
    icon: str
    description: str
    duration_hours: int
    interval_minutes: int
    min_messages_between_posts: int
    activity_check_interval_seconds: int
    visibility_max_wait_seconds: int


BROADCAST_PROFILES: dict[str, BroadcastProfile] = {
    "active": BroadcastProfile(
        key="active",
        title="Активная",
        icon="⚡",
        description="быстро возвращает пост наверх: 10 сообщений или максимум 15 минут",
        duration_hours=24,
        interval_minutes=5,
        min_messages_between_posts=10,
        activity_check_interval_seconds=60,
        visibility_max_wait_seconds=15 * 60,
    ),
    "balanced": BroadcastProfile(
        key="balanced",
        title="Средняя",
        icon="⚖️",
        description="ровный охват: 30 сообщений или максимум 1 час",
        duration_hours=48,
        interval_minutes=20,
        min_messages_between_posts=30,
        activity_check_interval_seconds=3 * 60,
        visibility_max_wait_seconds=3600,
    ),
    "gentle": BroadcastProfile(
        key="gentle",
        title="Бережная",
        icon="🌙",
        description="спокойная работа: 75 сообщений или максимум 3 часа",
        duration_hours=72,
        interval_minutes=60,
        min_messages_between_posts=75,
        activity_check_interval_seconds=10 * 60,
        visibility_max_wait_seconds=3 * 3600,
    ),
}


def broadcast_profile(key: str) -> BroadcastProfile:
    try:
        return BROADCAST_PROFILES[key]
    except KeyError as exc:
        raise ValueError("unknown broadcast profile") from exc


def detect_broadcast_profile(
    *,
    duration_seconds: int,
    interval_seconds: int,
    min_messages_between_posts: int,
    activity_check_interval_seconds: int,
    visibility_max_wait_seconds: int,
) -> BroadcastProfile | None:
    for profile in BROADCAST_PROFILES.values():
        if (
            duration_seconds == profile.duration_hours * 3600
            and interval_seconds == profile.interval_minutes * 60
            and min_messages_between_posts == profile.min_messages_between_posts
            and activity_check_interval_seconds == profile.activity_check_interval_seconds
            and visibility_max_wait_seconds == profile.visibility_max_wait_seconds
        ):
            return profile
    return None


@dataclass(frozen=True, slots=True)
class CapacityEstimate:
    messages_per_day: int | None
    requested_per_day: int
    full_round_seconds: int | None

    @property
    def coverage_ratio(self) -> float | None:
        if self.messages_per_day is None or self.requested_per_day <= 0:
            return None
        return min(1.0, self.messages_per_day / self.requested_per_day)


def estimate_capacity(
    *,
    account_count: int,
    target_count: int,
    interval_seconds: int,
    pace_seconds: float,
    daily_limit: int,
    micro_break_every: int,
    micro_break_seconds: float,
) -> CapacityEstimate:
    accounts = max(1, account_count)
    effective_seconds = max(0.0, pace_seconds)
    if micro_break_every > 0:
        effective_seconds += max(0.0, micro_break_seconds) / micro_break_every
    pace_capacity = max(1, floor(86400 / effective_seconds)) if effective_seconds > 0 else None
    per_account = pace_capacity
    if daily_limit > 0:
        per_account = daily_limit if per_account is None else min(per_account, daily_limit)
    messages_per_day = per_account * accounts if per_account is not None else None
    requested_per_day = max(1, target_count) * max(1, floor(86400 / max(60, interval_seconds)))
    full_round_seconds = (
        None
        if messages_per_day is None
        else max(1, round(max(1, target_count) * 86400 / messages_per_day))
    )
    return CapacityEstimate(
        messages_per_day=messages_per_day,
        requested_per_day=requested_per_day,
        full_round_seconds=full_round_seconds,
    )


@dataclass(slots=True)
class AccountCapacitySlot:
    """Deterministic virtual timeline used to avoid immediately-due queue churn."""

    available_at: datetime
    day_window_start: datetime
    used_in_base_window: int
    reserved: int = 0
    since_break: int = 0

    @classmethod
    def from_account(
        cls,
        account: Account,
        now: datetime,
        *,
        daily_limit: int = 0,
    ) -> AccountCapacitySlot:
        window_start = ensure_utc(account.day_window_start)
        if window_start is None or now - window_start >= timedelta(hours=24):
            window_start = now
            used = 0
        else:
            used = max(0, int(account.sent_today or 0))
        cooldown_until = ensure_utc(account.cooldown_until)
        if account.cooldown_reason == "daily_limit" and daily_limit > 0 and used < daily_limit:
            cooldown_until = None
        account_ready = max(
            [
                now,
                *(
                    value
                    for value in (ensure_utc(account.next_send_at), cooldown_until)
                    if value is not None
                ),
            ]
        )
        return cls(
            available_at=account_ready,
            day_window_start=window_start,
            used_in_base_window=used,
            since_break=max(0, int(account.sent_since_break or 0)),
        )

    def candidate_at(self, base: datetime, *, daily_limit: int) -> datetime:
        candidate = max(base, self.available_at)
        if daily_limit > 0:
            total_used = self.used_in_base_window + self.reserved
            window_number = total_used // daily_limit
            if window_number:
                candidate = max(
                    candidate,
                    self.day_window_start + timedelta(days=window_number),
                )
        return candidate

    def reserve(
        self,
        scheduled_for: datetime,
        *,
        pace_seconds: float,
        micro_break_every: int,
        micro_break_seconds: float,
    ) -> None:
        self.reserved += 1
        self.since_break += 1
        self.available_at = scheduled_for + timedelta(seconds=max(0.0, pace_seconds))
        if micro_break_every > 0 and self.since_break >= micro_break_every:
            self.available_at += timedelta(seconds=max(0.0, micro_break_seconds))
            self.since_break = 0
