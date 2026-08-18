"""Runtime health tracking and supervised restarts for background workers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from proxima_traffic.models import ensure_utc, utc_now

logger = logging.getLogger(__name__)

WorkerAlertSender = Callable[[str], Awaitable[None]]
WorkerIterationReporter = Callable[[BaseException | None], Awaitable[None]]
WorkerRunner = Callable[[WorkerIterationReporter], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class WorkerHealthSnapshot:
    name: str
    state: str
    consecutive_failures: int
    restart_count: int
    last_success_at: datetime | None
    last_error: str | None
    started_at: datetime | None = None


class WorkerSupervisor:
    """Keeps worker failures visible, alerts operators and restarts crashed tasks."""

    def __init__(
        self,
        *,
        alert_sender: WorkerAlertSender | None = None,
        alert_cooldown_seconds: float = 300.0,
        restart_delay_seconds: float = 5.0,
    ) -> None:
        self._alert_sender = alert_sender
        self._alert_cooldown = timedelta(seconds=max(1.0, alert_cooldown_seconds))
        self._restart_delay = max(0.1, restart_delay_seconds)
        self._stop_event = asyncio.Event()
        self._states: dict[str, _WorkerHealthState] = {}

    def start(self, name: str, runner: WorkerRunner) -> asyncio.Task[None]:
        if name in self._states:
            raise ValueError(f"worker {name!r} is already supervised")
        self._states[name] = _WorkerHealthState(started_at=utc_now())
        return asyncio.create_task(self._run(name, runner), name=f"supervised-{name}")

    def stop(self) -> None:
        self._stop_event.set()

    def snapshots(self) -> tuple[WorkerHealthSnapshot, ...]:
        return tuple(
            WorkerHealthSnapshot(
                name=name,
                state=state.state,
                consecutive_failures=state.consecutive_failures,
                restart_count=state.restart_count,
                last_success_at=state.last_success_at,
                last_error=state.last_error,
                started_at=state.started_at,
            )
            for name, state in self._states.items()
        )

    def ready(self, *, max_stale_seconds: float) -> bool:
        """Return false for failed, degraded, stopped, or stale supervised workers."""
        if not self._states:
            return True
        now = utc_now()
        maximum_age = timedelta(seconds=max(1.0, max_stale_seconds))
        for state in self._states.values():
            if state.state != "running":
                return False
            reference = ensure_utc(state.last_success_at or state.started_at)
            if reference is None or now - reference > maximum_age:
                return False
        return True

    async def _run(self, name: str, runner: WorkerRunner) -> None:
        while not self._stop_event.is_set():
            state = self._states[name]
            if state.state == "starting":
                state.state = "running"
            try:
                await runner(self._reporter(name))
            except asyncio.CancelledError:
                state.state = "stopped"
                raise
            except Exception as exc:  # noqa: BLE001 - workers must restart after an unexpected crash
                state.restart_count += 1
                await self._record_failure(name, exc, state="failed")
            else:
                if self._stop_event.is_set():
                    break
                state.restart_count += 1
                await self._record_failure(
                    name,
                    RuntimeError("worker stopped unexpectedly"),
                    state="failed",
                )
            if self._stop_event.is_set():
                break
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._restart_delay)
            except TimeoutError:
                pass
        self._states[name].state = "stopped"

    def _reporter(self, name: str) -> WorkerIterationReporter:
        async def report(error: BaseException | None = None) -> None:
            if error is None:
                state = self._states[name]
                state.state = "running"
                state.consecutive_failures = 0
                state.last_success_at = utc_now()
                state.last_error = None
                return
            await self._record_failure(name, error, state="degraded")

        return report

    async def _record_failure(self, name: str, error: BaseException, *, state: str) -> None:
        health = self._states[name]
        health.state = state
        health.consecutive_failures += 1
        health.last_error = _error_text(error)
        now = utc_now()
        if (
            self._alert_sender is None
            or health.last_alert_at is not None
            and (now - ensure_utc(health.last_alert_at)) < self._alert_cooldown
        ):
            return
        health.last_alert_at = now
        try:
            await self._alert_sender(
                "⚠️ <b>Фоновый worker деградировал</b>\n"
                f"Сервис: <code>{name}</code>\n"
                f"Статус: <b>{state}</b>\n"
                f"Ошибка: <code>{health.last_error}</code>"
            )
        except Exception:  # noqa: BLE001 - monitoring must not kill the worker it observes
            logger.exception("Could not send health alert for worker %s", name)


@dataclass(slots=True)
class _WorkerHealthState:
    state: str = "starting"
    consecutive_failures: int = 0
    restart_count: int = 0
    last_success_at: datetime | None = None
    last_error: str | None = None
    last_alert_at: datetime | None = None
    started_at: datetime | None = None


def _error_text(error: BaseException) -> str:
    message = " ".join(str(error).split())
    return (message or type(error).__name__)[:250]
