import gzip
from pathlib import Path

import pytest

import proxima_traffic.subscriber_import as subscriber_import
from proxima_traffic.subscriber_import import (
    MAX_SUBSCRIBER_LINE_BYTES,
    SubscriberImportError,
    SubscriberParseStats,
    iter_subscriber_records,
)


def test_corrupted_gzip_read_is_reported_as_subscriber_import_error(tmp_path: Path) -> None:
    gzip_path = tmp_path / "truncated-subscribers.jsonl.gz"
    payload = gzip.compress(
        b'{"telegram_id":"2050","username":"truncated_member","is_premium":false}\n'
    )
    # Keep a valid GZIP header/body but remove the trailer. ``gzip.open`` succeeds;
    # the failure appears only while the yielded stream is being consumed.
    gzip_path.write_bytes(payload[:-6])

    with pytest.raises(SubscriberImportError, match="повреждена|не читается"):
        list(iter_subscriber_records(gzip_path))


def test_oversized_line_is_drained_and_next_record_is_still_imported(tmp_path: Path) -> None:
    source = tmp_path / "oversized.jsonl"
    source.write_bytes(
        b"x" * (MAX_SUBSCRIBER_LINE_BYTES * 3)
        + b"\n"
        + b'{"telegram_id":"2051","username":"bounded_member","is_premium":true}\n'
    )
    stats = SubscriberParseStats()

    records = list(iter_subscriber_records(source, stats=stats))

    assert [item.telegram_user_id for item in records] == [2051]
    assert stats.total_lines == 2
    assert stats.valid_records == 1
    assert stats.invalid == 1
    assert stats.issues[0].reason == "строка больше 64 КБ"
    assert stats.decoded_bytes == source.stat().st_size


def test_decoded_byte_budget_stops_oversized_stream_before_end_of_line(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "budget.jsonl"
    source.write_bytes(b"x" * 4_096)
    monkeypatch.setattr(subscriber_import, "MAX_SUBSCRIBER_IMPORT_BYTES", 512)

    with pytest.raises(SubscriberImportError, match="256 МБ"):
        list(iter_subscriber_records(source))
