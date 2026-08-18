from proxima_traffic.scheduling import estimate_capacity


def test_capacity_is_zero_without_accounts_or_targets() -> None:
    no_accounts = estimate_capacity(
        account_count=0,
        target_count=5,
        interval_seconds=300,
        pace_seconds=1,
        daily_limit=0,
        micro_break_every=0,
        micro_break_seconds=0,
    )
    no_targets = estimate_capacity(
        account_count=2,
        target_count=0,
        interval_seconds=300,
        pace_seconds=1,
        daily_limit=0,
        micro_break_every=0,
        micro_break_seconds=0,
    )
    assert no_accounts.messages_per_day == 0
    assert no_accounts.requested_per_day == 0
    assert no_accounts.full_round_seconds is None
    assert no_targets.messages_per_day == 0
    assert no_targets.requested_per_day == 0
    assert no_targets.full_round_seconds is None
