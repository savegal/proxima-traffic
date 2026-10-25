"""Supported private-chat settings and pacing for service-action tasks."""

from random import SystemRandom

DIRECT_SERVICE_ACTIONS = frozenset({"auto_delete", "protect_content"})
AUTO_DELETE_PERIODS = {0: "Выключить таймер", 86400: "1 день", 604800: "7 дней", 2592000: "30 дней"}


def service_action_delay_seconds() -> float:
    return SystemRandom().uniform(60, 600)


def service_action_label(kind: str, period: int | None = None) -> str:
    if kind == "protect_content":
        return "Запретить копирование и пересылку"
    if kind == "auto_delete":
        return "Автоудаление: " + AUTO_DELETE_PERIODS.get(period, "неизвестный период")
    return "Личное сообщение"
