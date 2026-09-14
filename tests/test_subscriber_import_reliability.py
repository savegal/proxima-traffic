import gzip
from pathlib import Path

import pytest

from proxima_traffic.subscriber_import import SubscriberImportError, iter_subscriber_records


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
