import gzip
import io

import pytest

from proxima_traffic import subscriber_import


class BoundedStream(io.BytesIO):
    def readline(self, size=-1):
        assert 0 < size <= subscriber_import.MAX_SUBSCRIBER_LINE_BYTES + 1
        return super().readline(size)


@pytest.mark.parametrize("newline", [b"\n", b"\r\n"])
def test_oversized_subscriber_line_is_drained_without_buffering_the_whole_line(
    monkeypatch, newline
):
    oversized = b"x" * (subscriber_import.MAX_SUBSCRIBER_LINE_BYTES * 3)
    valid = b'{"id":12345,"username":"recipient_one"}' + newline
    stream = BoundedStream(oversized + newline + valid)
    monkeypatch.setattr(
        subscriber_import, "_subscriber_streams", lambda _path: [("part.jsonl", stream)]
    )
    stats = subscriber_import.SubscriberParseStats()
    records = list(subscriber_import.iter_subscriber_records("unused.jsonl", stats=stats))
    assert len(records) == 1
    assert records[0].telegram_user_id == 12345
    assert stats.invalid == 1
    assert stats.total_lines == 2
    assert stats.decoded_bytes == len(oversized + newline + valid)


def test_expansion_limit_is_checked_while_draining_an_oversized_line(monkeypatch):
    limit = subscriber_import.MAX_SUBSCRIBER_LINE_BYTES * 2
    stream = BoundedStream(b"x" * (limit * 10))
    monkeypatch.setattr(subscriber_import, "MAX_SUBSCRIBER_IMPORT_BYTES", limit)
    monkeypatch.setattr(
        subscriber_import, "_subscriber_streams", lambda _path: [("part.jsonl", stream)]
    )
    with pytest.raises(subscriber_import.SubscriberImportError, match="256 МБ"):
        list(subscriber_import.iter_subscriber_records("unused.jsonl"))
    assert stream.tell() <= limit + subscriber_import.MAX_SUBSCRIBER_LINE_BYTES + 1


def test_truncated_gzip_has_a_clear_import_error(tmp_path):
    path = tmp_path / "truncated.jsonl.gz"
    path.write_bytes(gzip.compress(b'{"id":12345}\n')[:-8])
    with pytest.raises(subscriber_import.SubscriberImportError, match="повреждён"):
        list(subscriber_import.iter_subscriber_records(path))
