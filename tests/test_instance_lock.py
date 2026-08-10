from __future__ import annotations

from pathlib import Path

import pytest

from proxima_traffic.instance_lock import BotInstanceAlreadyRunning, BotInstanceLock


def test_bot_instance_lock_rejects_a_second_process_owner(tmp_path: Path) -> None:
    path = tmp_path / "control-bot.lock"
    first = BotInstanceLock(path)
    second = BotInstanceLock(path)
    first.acquire()
    try:
        with pytest.raises(BotInstanceAlreadyRunning):
            second.acquire()
    finally:
        first.release()

    second.acquire()
    second.release()
