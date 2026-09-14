"""Streaming, bounded import of large Telegram subscriber databases."""

from __future__ import annotations

import gzip
import json
import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

MAX_SUBSCRIBER_IMPORT_RECORDS = 2_000_000
MAX_SUBSCRIBER_IMPORT_BYTES = 256 * 1024 * 1024
MAX_SUBSCRIBER_LINE_BYTES = 64 * 1024
MAX_SUBSCRIBER_ARCHIVE_FILES = 1_000
MAX_IMPORT_ISSUES = 20
SUPPORTED_SUBSCRIBER_SUFFIXES = (".jsonl", ".jsonl.gz", ".gz", ".zip")
USERNAME_PATTERN = re.compile(r"[A-Za-z0-9_]{5,32}")


class SubscriberImportError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class SubscriberRecord:
    telegram_user_id: int
    username: str | None
    is_premium: bool | None


@dataclass(frozen=True, slots=True)
class SubscriberImportIssue:
    source: str
    line_number: int
    reason: str


@dataclass(slots=True)
class SubscriberParseStats:
    total_lines: int = 0
    valid_records: int = 0
    duplicates: int = 0
    invalid: int = 0
    blank: int = 0
    decoded_bytes: int = 0
    issues: list[SubscriberImportIssue] = field(default_factory=list)

    def add_issue(self, source: str, line_number: int, reason: str) -> None:
        self.invalid += 1
        if len(self.issues) < MAX_IMPORT_ISSUES:
            self.issues.append(
                SubscriberImportIssue(
                    source=Path(source).name[:120],
                    line_number=line_number,
                    reason=reason[:200],
                )
            )


def subscriber_file_supported(file_name: str) -> bool:
    normalized = file_name.strip().casefold()
    return any(normalized.endswith(suffix) for suffix in SUPPORTED_SUBSCRIBER_SUFFIXES)


def subscriber_list_name(file_name: str) -> str:
    """Produce a stable human name while removing export timestamps/noise."""

    name = Path(file_name).name
    lowered = name.casefold()
    for suffix in sorted(SUPPORTED_SUBSCRIBER_SUFFIXES, key=len, reverse=True):
        if lowered.endswith(suffix):
            name = name[: -len(suffix)]
            break
    name = re.sub(
        r"(?i)(?:[_-]jsonl)?[_-]\d{8}[_-]\d{6}(?:[_-][a-z0-9]{4,})?$",
        "",
        name,
    )
    name = re.sub(r"[_\-]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return (name or "База подписчиков")[:150]


def iter_subscriber_records(
    path: str | Path,
    *,
    stats: SubscriberParseStats | None = None,
) -> Iterator[SubscriberRecord]:
    """Yield normalized records without retaining the whole source file in memory."""

    import_stats = stats if stats is not None else SubscriberParseStats()
    seen_ids: set[int] = set()
    for source_name, stream in _subscriber_streams(Path(path)):
        for line_number, (raw_line, line_size, oversized) in enumerate(
            _iter_stream_lines(source_name, stream),
            start=1,
        ):
            import_stats.total_lines += 1
            if import_stats.total_lines > MAX_SUBSCRIBER_IMPORT_RECORDS:
                raise SubscriberImportError(
                    f"в одном импорте допускается не больше {MAX_SUBSCRIBER_IMPORT_RECORDS:,} строк"
                )
            import_stats.decoded_bytes += line_size
            if import_stats.decoded_bytes > MAX_SUBSCRIBER_IMPORT_BYTES:
                raise SubscriberImportError("распакованный файл превышает безопасный лимит 256 МБ")
            if oversized:
                import_stats.add_issue(source_name, line_number, "строка больше 64 КБ")
                continue
            raw_line = raw_line.strip()
            if not raw_line:
                import_stats.blank += 1
                continue
            # Every ZIP member is an independent UTF-8 JSONL export and may
            # carry its own BOM; using the global line count rejects member 2.
            if line_number == 1:
                raw_line = raw_line.removeprefix(b"\xef\xbb\xbf")
            try:
                decoded = raw_line.decode("utf-8", errors="strict")
            except UnicodeDecodeError:
                import_stats.add_issue(source_name, line_number, "строка не в UTF-8")
                continue
            try:
                value = json.loads(decoded)
            except json.JSONDecodeError:
                import_stats.add_issue(source_name, line_number, "некорректный JSON")
                continue
            try:
                record = _normalize_record(value)
            except SubscriberImportError as exc:
                import_stats.add_issue(source_name, line_number, str(exc))
                continue
            if record.telegram_user_id in seen_ids:
                import_stats.duplicates += 1
                continue
            seen_ids.add(record.telegram_user_id)
            import_stats.valid_records += 1
            yield record


def _read_stream_chunk(source_name: str, stream: BinaryIO, limit: int) -> bytes:
    """Read one bounded line chunk and normalize lazy archive failures."""

    try:
        return stream.readline(limit)
    except (gzip.BadGzipFile, zipfile.BadZipFile, EOFError, OSError) as exc:
        source = Path(source_name).name[:120] or "файл импорта"
        raise SubscriberImportError(f"часть {source!r} повреждена или не читается") from exc


def _iter_stream_lines(source_name: str, stream: BinaryIO) -> Iterator[tuple[bytes, int, bool]]:
    """Yield physical lines with bounded memory and exact decoded-byte accounting.

    ``BinaryIO`` iteration and an unbounded ``readline()`` allocate the whole
    physical line before the caller can enforce ``MAX_SUBSCRIBER_LINE_BYTES``.
    A malformed export containing one enormous line could therefore consume
    hundreds of megabytes despite the documented 64 KiB line limit. Read at
    most ``limit + 1`` bytes at a time and drain the rest of an oversized line
    in bounded chunks.

    Archive readers are lazy as well, so decompression/CRC failures are caught
    at each actual read rather than only when the stream is opened.
    """

    chunk_limit = MAX_SUBSCRIBER_LINE_BYTES + 1
    while True:
        first = _read_stream_chunk(source_name, stream, chunk_limit)
        if not first:
            return
        line_size = len(first)
        oversized = line_size > MAX_SUBSCRIBER_LINE_BYTES
        complete = first.endswith(b"\n")
        if not complete:
            while True:
                tail = _read_stream_chunk(source_name, stream, chunk_limit)
                if not tail:
                    break
                line_size += len(tail)
                if line_size > MAX_SUBSCRIBER_LINE_BYTES:
                    oversized = True
                if tail.endswith(b"\n"):
                    break
        yield first, line_size, oversized


def _normalize_record(value: object) -> SubscriberRecord:
    if not isinstance(value, dict):
        raise SubscriberImportError("строка должна содержать JSON-объект")
    raw_user_id = value.get("telegram_id", value.get("user_id", value.get("id")))
    if isinstance(raw_user_id, bool):
        raise SubscriberImportError("telegram_id должен быть положительным числом")
    try:
        user_id = int(str(raw_user_id).strip())
    except (TypeError, ValueError) as exc:
        raise SubscriberImportError("telegram_id должен быть положительным числом") from exc
    if not 1 <= user_id <= 9_223_372_036_854_775_807:
        raise SubscriberImportError("telegram_id находится вне допустимого диапазона")

    raw_username = value.get("username")
    username: str | None = None
    if raw_username is not None:
        candidate = str(raw_username).strip().lstrip("@").casefold()
        if candidate:
            if USERNAME_PATTERN.fullmatch(candidate) is None:
                raise SubscriberImportError("username имеет недопустимый формат")
            username = candidate

    premium_value = value.get("is_premium", value.get("premium"))
    if premium_value is not None and not isinstance(premium_value, bool):
        raise SubscriberImportError("is_premium должен быть true, false или null")
    return SubscriberRecord(
        telegram_user_id=user_id,
        username=username,
        is_premium=premium_value,
    )


def _subscriber_streams(path: Path) -> Iterator[tuple[str, BinaryIO]]:
    if not path.is_file():
        raise SubscriberImportError("файл импорта не найден")
    lowered = path.name.casefold()
    if lowered.endswith(".zip"):
        try:
            with zipfile.ZipFile(path) as archive:
                entries = [
                    item
                    for item in archive.infolist()
                    if not item.is_dir() and item.filename.casefold().endswith(".jsonl")
                ]
                if not entries:
                    raise SubscriberImportError("в ZIP нет файлов .jsonl")
                if len(entries) > MAX_SUBSCRIBER_ARCHIVE_FILES:
                    raise SubscriberImportError("в ZIP слишком много частей")
                if any(item.flag_bits & 0x1 for item in entries):
                    raise SubscriberImportError("ZIP с паролем не поддерживается")
                if sum(item.file_size for item in entries) > MAX_SUBSCRIBER_IMPORT_BYTES:
                    raise SubscriberImportError("распакованный ZIP превышает 256 МБ")
                for entry in sorted(entries, key=lambda item: item.filename.casefold()):
                    with archive.open(entry, "r") as stream:
                        yield entry.filename, stream
        except (zipfile.BadZipFile, OSError) as exc:
            raise SubscriberImportError("ZIP повреждён или не читается") from exc
        return
    if lowered.endswith((".jsonl.gz", ".gz")):
        try:
            with gzip.open(path, "rb") as stream:
                yield path.name, stream
        except (gzip.BadGzipFile, EOFError, OSError) as exc:
            raise SubscriberImportError("GZIP повреждён или не читается") from exc
        return
    if not lowered.endswith(".jsonl"):
        raise SubscriberImportError("поддерживаются .jsonl, .jsonl.gz и .zip")
    try:
        with path.open("rb") as stream:
            yield path.name, stream
    except OSError as exc:
        raise SubscriberImportError("JSONL повреждён или не читается") from exc
