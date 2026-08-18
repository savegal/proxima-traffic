from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from proxima_traffic.content import ALLOWED_MEDIA_KINDS, validate_spintax

MAX_DIALOGUE_STEPS = 20
MAX_DIALOGUE_STEP_DELAY_SECONDS = 24 * 3600


@dataclass(frozen=True, slots=True)
class DialogueStep:
    account_id: int
    text: str
    delay_seconds: int = 0
    reply_to_previous: bool = True
    media_path: str | None = None
    media_kind: str | None = None
    media_name: str | None = None
    media_size: int | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def validate_dialogue_steps(raw_steps: list[dict[str, Any]]) -> list[DialogueStep]:
    if not 2 <= len(raw_steps) <= MAX_DIALOGUE_STEPS:
        raise ValueError(f"диалог должен содержать от 2 до {MAX_DIALOGUE_STEPS} сообщений")
    steps: list[DialogueStep] = []
    for index, raw in enumerate(raw_steps):
        try:
            account_id = int(raw["account_id"])
            text = str(raw["text"]).strip()
            delay_seconds = int(raw.get("delay_seconds", 0))
            raw_reply_to_previous = raw.get("reply_to_previous", index > 0)
            if not isinstance(raw_reply_to_previous, bool):
                raise ValueError("reply_to_previous must be a boolean")
            reply_to_previous = raw_reply_to_previous
            media_path = str(raw["media_path"]) if raw.get("media_path") else None
            media_kind = str(raw["media_kind"]) if raw.get("media_kind") else None
            media_name = str(raw["media_name"]) if raw.get("media_name") else None
            media_size = int(raw["media_size"]) if raw.get("media_size") is not None else None
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"неверные параметры сообщения диалога № {index + 1}") from exc
        if account_id <= 0:
            raise ValueError(f"у сообщения диалога № {index + 1} неверный аккаунт")
        if (media_path is None) != (media_kind is None):
            raise ValueError(f"у сообщения диалога № {index + 1} повреждены данные медиа")
        if media_path is None and (media_name is not None or media_size is not None):
            raise ValueError(
                f"у сообщения диалога № {index + 1} метаданные медиа указаны без файла"
            )
        if media_kind is not None and media_kind not in ALLOWED_MEDIA_KINDS:
            raise ValueError(f"у сообщения диалога № {index + 1} неизвестный тип медиа")
        allows_empty_text = media_kind is not None
        maximum_text = 1024 if media_kind is not None else 4096
        if not 1 <= len(text) <= maximum_text and not (allows_empty_text and not text):
            raise ValueError(
                f"сообщение диалога № {index + 1} должно содержать от 1 до {maximum_text} символов"
            )
        if media_kind == "video_note" and text:
            raise ValueError(f"к видеокружку в шаге № {index + 1} нельзя добавить подпись")
        if media_size is not None and media_size < 0:
            raise ValueError(f"у сообщения диалога № {index + 1} неверный размер медиа")
        if media_name is not None and len(media_name) > 255:
            raise ValueError(f"у сообщения диалога № {index + 1} слишком длинное имя медиа")
        try:
            validate_spintax(text)
        except ValueError as exc:
            raise ValueError(f"сообщение диалога № {index + 1}: {exc}") from exc
        if not 0 <= delay_seconds <= MAX_DIALOGUE_STEP_DELAY_SECONDS:
            raise ValueError(
                f"задержка сообщения диалога № {index + 1} должна быть от 0 до "
                f"{MAX_DIALOGUE_STEP_DELAY_SECONDS} секунд"
            )
        if index == 0:
            delay_seconds = 0
            reply_to_previous = False
        steps.append(
            DialogueStep(
                account_id=account_id,
                text=text,
                delay_seconds=delay_seconds,
                reply_to_previous=reply_to_previous,
                media_path=media_path,
                media_kind=media_kind,
                media_name=media_name,
                media_size=media_size,
            )
        )
    if len({step.account_id for step in steps}) < 2:
        raise ValueError("для диалога нужны хотя бы два разных аккаунта")
    if any(
        current.account_id == previous.account_id
        for previous, current in zip(steps, steps[1:], strict=False)
    ):
        raise ValueError("соседние сообщения диалога должны отправлять разные аккаунты")
    return steps


def serialize_dialogue_steps(steps: list[DialogueStep]) -> str:
    return json.dumps(
        [step.as_dict() for step in steps],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def parse_dialogue_steps(
    value: str | None,
    *,
    primary_account_id: int,
    primary_text: str,
    reply_account_id: int | None,
    reply_text: str,
    reply_delay_seconds: int,
) -> list[DialogueStep]:
    if value:
        try:
            raw = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("сохранённая цепочка диалога повреждена") from exc
        if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
            raise ValueError("сохранённая цепочка диалога имеет неверный формат")
        return validate_dialogue_steps(raw)
    if reply_account_id is None:
        raise ValueError("для диалога нужен аккаунт ответа")
    return validate_dialogue_steps(
        [
            {
                "account_id": primary_account_id,
                "text": primary_text,
                "delay_seconds": 0,
                "reply_to_previous": False,
            },
            {
                "account_id": reply_account_id,
                "text": reply_text,
                "delay_seconds": reply_delay_seconds,
                "reply_to_previous": True,
            },
        ]
    )
