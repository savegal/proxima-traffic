"""Cross-process lock for the Telegram control bot polling loop."""

from __future__ import annotations

import hashlib
import json
import os
import socket
from pathlib import Path
from typing import BinaryIO

from proxima_traffic.config import default_data_directory


class BotInstanceAlreadyRunning(RuntimeError):
    """Raised when this machine already owns the control-bot polling lock."""


def control_bot_lock_path(bot_token: str) -> Path:
    """Return a stable path without exposing the Bot API token."""
    token_fingerprint = hashlib.sha256(bot_token.encode("utf-8")).hexdigest()[:20]
    return default_data_directory() / "locks" / f"control-bot-{token_fingerprint}.lock"


class BotInstanceLock:
    """Hold one non-blocking OS file lock for the lifetime of a bot process."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: BinaryIO | None = None

    def acquire(self) -> None:
        if self._handle is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            self._ensure_lock_byte(handle)
            self._lock(handle)
        except OSError as exc:
            handle.close()
            raise BotInstanceAlreadyRunning(
                "другой экземпляр Proxima уже получает обновления этого Telegram-бота"
            ) from exc
        self._handle = handle
        self._write_owner_metadata(handle)

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        try:
            handle.seek(0)
            self._unlock(handle)
        finally:
            handle.close()

    def __enter__(self) -> BotInstanceLock:
        self.acquire()
        return self

    def __exit__(self, *args: object) -> None:
        del args
        self.release()

    @staticmethod
    def _ensure_lock_byte(handle: BinaryIO) -> None:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)

    @staticmethod
    def _lock(handle: BinaryIO) -> None:
        if os.name == "nt":
            import msvcrt  # noqa: PLC0415

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return
        import fcntl  # noqa: PLC0415

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock(handle: BinaryIO) -> None:
        if os.name == "nt":
            import msvcrt  # noqa: PLC0415

            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            return
        import fcntl  # noqa: PLC0415

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _write_owner_metadata(handle: BinaryIO) -> None:
        metadata = json.dumps(
            {"pid": os.getpid(), "host": socket.gethostname()},
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        handle.seek(1)
        handle.truncate(1)
        handle.write(metadata)
        handle.flush()
        os.fsync(handle.fileno())
