from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from proxima_traffic.config import Settings

ALLOWED_MEDIA_KINDS = frozenset({"photo", "video", "document", "voice", "video_note"})
SINGLE_MEDIA_KINDS = frozenset({"document", "voice", "video_note"})
MAX_CONTENT_VARIANTS = 10
MAX_ALBUM_ITEMS = 10
MAX_SPINTAX_GROUPS = 100
MAX_SPINTAX_DEPTH = 8


class ContentError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class _SpintaxGroup:
    index: int
    alternatives: tuple[tuple[str | _SpintaxGroup, ...], ...]


class _SpintaxParser:
    """Small strict parser for `{one|two}` with nested groups and escapes."""

    def __init__(self, value: str) -> None:
        self._value = value
        self._position = 0
        self._group_count = 0

    @property
    def group_count(self) -> int:
        return self._group_count

    def parse(self) -> tuple[str | _SpintaxGroup, ...]:
        return self._sequence(stops=frozenset(), depth=0)

    def _sequence(
        self,
        *,
        stops: frozenset[str],
        depth: int,
    ) -> tuple[str | _SpintaxGroup, ...]:
        nodes: list[str | _SpintaxGroup] = []
        literal: list[str] = []
        while self._position < len(self._value):
            character = self._value[self._position]
            if character in stops:
                break
            if character == "\\":
                literal.append(self._escaped_character())
                continue
            if character == "{":
                if literal:
                    nodes.append("".join(literal))
                    literal.clear()
                nodes.append(self._group(depth=depth + 1))
                continue
            if character == "}":
                raise ContentError("Spintax: лишняя закрывающая скобка `}`")
            literal.append(character)
            self._position += 1
        if literal:
            nodes.append("".join(literal))
        return tuple(nodes)

    def _escaped_character(self) -> str:
        self._position += 1
        if self._position >= len(self._value):
            return "\\"
        character = self._value[self._position]
        self._position += 1
        return character if character in "{}|\\" else f"\\{character}"

    def _group(self, *, depth: int) -> _SpintaxGroup:
        if depth > MAX_SPINTAX_DEPTH:
            raise ContentError(
                f"Spintax: вложенность не должна быть больше {MAX_SPINTAX_DEPTH} уровней"
            )
        if self._group_count >= MAX_SPINTAX_GROUPS:
            raise ContentError(
                f"Spintax: в одном сообщении допускается не больше {MAX_SPINTAX_GROUPS} блоков"
            )
        group_index = self._group_count
        self._group_count += 1
        self._position += 1  # opening {
        alternatives: list[tuple[str | _SpintaxGroup, ...]] = []
        while True:
            alternatives.append(self._sequence(stops=frozenset({"|", "}"}), depth=depth))
            if self._position >= len(self._value):
                raise ContentError("Spintax: не закрыта скобка `{`")
            delimiter = self._value[self._position]
            self._position += 1
            if delimiter == "}":
                break
        if len(alternatives) < 2:
            raise ContentError("Spintax: в блоке `{...}` нужно минимум два варианта через `|`")
        return _SpintaxGroup(index=group_index, alternatives=tuple(alternatives))


def _parse_spintax(value: str) -> tuple[tuple[str | _SpintaxGroup, ...], int]:
    parser = _SpintaxParser(value)
    return parser.parse(), parser.group_count


def validate_spintax(value: str) -> None:
    """Validate syntax early, before an operator can launch a campaign."""
    _parse_spintax(value)


def spintax_group_count(value: str) -> int:
    """Return the number of expandable groups in one message template."""
    _, count = _parse_spintax(value)
    return count


def render_spintax(value: str, *, seed: str) -> str:
    """Render one deterministic Spintax result for an immutable delivery key.

    The same seed always produces the same final text.  This is essential for
    retries: a network failure must never turn one queued message into a new
    variation with the same Telegram random ID.
    """
    nodes, _ = _parse_spintax(value)

    def render(nodes_to_render: tuple[str | _SpintaxGroup, ...]) -> str:
        parts: list[str] = []
        for node in nodes_to_render:
            if isinstance(node, str):
                parts.append(node)
                continue
            digest = sha256(f"{seed}:{node.index}".encode()).digest()
            choice = int.from_bytes(digest[:8], "big") % len(node.alternatives)
            parts.append(render(node.alternatives[choice]))
        return "".join(parts)

    return render(nodes)


@dataclass(frozen=True, slots=True)
class StoredMedia:
    path: str
    kind: str
    name: str
    size: int

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "kind": self.kind,
            "name": self.name,
            "size": self.size,
        }


@dataclass(frozen=True, slots=True)
class StoredContact:
    phone_number: str
    first_name: str
    last_name: str = ""
    vcard: str = ""

    @property
    def display_name(self) -> str:
        return " ".join(part for part in (self.first_name, self.last_name) if part)

    def as_dict(self) -> dict[str, str]:
        return {
            "phone_number": self.phone_number,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "vcard": self.vcard,
        }


@dataclass(frozen=True, slots=True)
class BroadcastContentVariant:
    variant_id: str
    name: str
    text: str
    html_text: str | None = None
    media: tuple[StoredMedia, ...] = ()
    contact: StoredContact | None = None
    enabled: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.variant_id,
            "name": self.name,
            "text": self.text,
            "html": self.html_text,
            "media": [item.as_dict() for item in self.media],
            "contact": self.contact.as_dict() if self.contact else None,
            "enabled": self.enabled,
        }


def parse_content_variants(
    raw: str | None,
    *,
    fallback_text: str,
    fallback_html: str | None = None,
    fallback_media_path: str | None = None,
    fallback_media_kind: str | None = None,
    fallback_media_name: str | None = None,
    fallback_media_size: int | None = None,
) -> list[BroadcastContentVariant]:
    if raw:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ContentError("набор вариантов контента повреждён") from exc
        if not isinstance(payload, list):
            raise ContentError("набор вариантов контента должен быть списком")
        variants = [_parse_variant(item) for item in payload]
        _validate_variant_set(variants)
        return variants

    media: tuple[StoredMedia, ...] = ()
    if fallback_media_path:
        media = (
            StoredMedia(
                path=fallback_media_path,
                kind=str(fallback_media_kind or ""),
                name=str(fallback_media_name or "media"),
                size=int(fallback_media_size or 0),
            ),
        )
    variant = BroadcastContentVariant(
        variant_id="primary",
        name="Основной",
        text=fallback_text,
        html_text=fallback_html,
        media=media,
    )
    _validate_variant(variant)
    return [variant]


def serialize_content_variants(variants: list[BroadcastContentVariant]) -> str:
    _validate_variant_set(variants)
    return json.dumps(
        [variant.as_dict() for variant in variants],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def new_content_variant(
    *,
    name: str,
    text: str,
    html_text: str | None = None,
    media: tuple[StoredMedia, ...] = (),
    contact: StoredContact | None = None,
) -> BroadcastContentVariant:
    variant = BroadcastContentVariant(
        variant_id=uuid4().hex[:10],
        name=name.strip(),
        text=text.strip(),
        html_text=html_text,
        media=media,
        contact=contact,
    )
    _validate_variant(variant)
    return variant


def render_telegram_html(html_text: str) -> str:
    """Return the exact text Telegram will receive for a formatted payload."""

    try:
        from telethon.extensions import html

        rendered, _ = html.parse(html_text)
    except Exception as exc:  # noqa: BLE001 - normalize parser errors for all entry points
        raise ContentError("форматированный текст нельзя разобрать для Telegram") from exc
    return str(rendered)


def validate_telegram_text(
    text: str,
    html_text: str | None = None,
    *,
    has_media: bool = False,
    allow_empty: bool = False,
) -> str:
    """Validate one canonical Telegram message body and its optional markup."""

    validate_spintax(text)
    rendered = text
    if html_text is not None:
        validate_spintax(html_text)
        rendered = render_telegram_html(html_text)
        if rendered != text:
            raise ContentError(
                "форматированный текст после разбора должен совпадать с обычным текстом"
            )
    maximum_text = 1024 if has_media else 4096
    if allow_empty and not rendered:
        if html_text is not None:
            raise ContentError("пустое медиа-сообщение не может содержать форматирование")
        return rendered
    if not 1 <= len(rendered) <= maximum_text:
        raise ContentError(f"текст варианта должен содержать от 1 до {maximum_text} символов")
    return rendered


def _parse_variant(raw: object) -> BroadcastContentVariant:
    if not isinstance(raw, dict):
        raise ContentError("вариант контента должен быть объектом")
    raw_media = raw.get("media", [])
    if not isinstance(raw_media, list):
        raise ContentError("медиа варианта должно быть списком")
    media: list[StoredMedia] = []
    for item in raw_media:
        if not isinstance(item, dict):
            raise ContentError("элемент альбома должен быть объектом")
        try:
            stored = StoredMedia(
                path=str(item["path"]),
                kind=str(item["kind"]),
                name=str(item.get("name") or "media")[:255],
                size=int(item.get("size") or 0),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ContentError("элемент альбома содержит некорректные данные") from exc
        media.append(stored)
    raw_contact = raw.get("contact")
    contact = parse_stored_contact(raw_contact) if raw_contact is not None else None
    variant = BroadcastContentVariant(
        variant_id=str(raw.get("id") or "").strip(),
        name=str(raw.get("name") or "").strip(),
        text=str(raw.get("text") or "").strip(),
        html_text=(str(raw["html"]) if raw.get("html") is not None else None),
        media=tuple(media),
        contact=contact,
        enabled=bool(raw.get("enabled", True)),
    )
    _validate_variant(variant)
    return variant


def _validate_variant_set(variants: list[BroadcastContentVariant]) -> None:
    if not 1 <= len(variants) <= MAX_CONTENT_VARIANTS:
        raise ContentError(f"должно быть от 1 до {MAX_CONTENT_VARIANTS} вариантов")
    ids = [variant.variant_id for variant in variants]
    if len(ids) != len(set(ids)):
        raise ContentError("идентификаторы вариантов должны быть уникальны")
    if not any(variant.enabled for variant in variants):
        raise ContentError("хотя бы один вариант должен быть включён")
    for variant in variants:
        _validate_variant(variant)


def _validate_variant(variant: BroadcastContentVariant) -> None:
    if not variant.variant_id or len(variant.variant_id) > 40:
        raise ContentError("идентификатор варианта некорректен")
    if not 1 <= len(variant.name) <= 80:
        raise ContentError("название варианта должно содержать от 1 до 80 символов")
    media_can_be_sent_without_body = bool(variant.media)
    if not variant.text and variant.contact is None and not media_can_be_sent_without_body:
        raise ContentError("рекламный текст варианта не может быть пустым")
    if variant.contact is not None and (variant.text or variant.html_text or variant.media):
        raise ContentError("контакт должен быть отдельным вариантом без текста и медиа")
    if variant.html_text is not None and len(variant.html_text) > 16_384:
        raise ContentError("форматированный текст варианта слишком длинный")
    if variant.contact is None:
        validate_telegram_text(
            variant.text,
            variant.html_text,
            has_media=bool(variant.media),
            allow_empty=media_can_be_sent_without_body,
        )
    if len(variant.media) > MAX_ALBUM_ITEMS:
        raise ContentError(f"в альбоме может быть не больше {MAX_ALBUM_ITEMS} файлов")
    for item in variant.media:
        if item.kind not in ALLOWED_MEDIA_KINDS or not item.path or item.size < 0:
            raise ContentError("альбом содержит некорректный медиафайл")
    if len(variant.media) > 1 and any(item.kind in SINGLE_MEDIA_KINDS for item in variant.media):
        raise ContentError("документы, голосовые и кружки отправляются только по одному")
    if len(variant.media) == 1 and variant.media[0].kind == "video_note" and variant.text:
        raise ContentError("к видеокружку нельзя добавить подпись")
    if variant.contact is not None:
        _validate_contact(variant.contact)


def new_stored_contact(
    *,
    phone_number: str,
    first_name: str,
    last_name: str = "",
    vcard: str = "",
) -> StoredContact:
    raw_phone = phone_number.strip()
    if re.fullmatch(r"\+?[0-9 ().-]+", raw_phone) is None:
        raise ContentError("номер контакта содержит недопустимые символы")
    prefix = "+" if raw_phone.startswith("+") else ""
    digits = re.sub(r"\D", "", raw_phone)
    contact = StoredContact(
        phone_number=f"{prefix}{digits}",
        first_name=" ".join(first_name.split()),
        last_name=" ".join(last_name.split()),
        vcard=vcard.strip(),
    )
    _validate_contact(contact)
    return contact


def parse_stored_contact(raw: object) -> StoredContact:
    if not isinstance(raw, dict):
        raise ContentError("контакт варианта должен быть объектом")
    try:
        return new_stored_contact(
            phone_number=str(raw["phone_number"]),
            first_name=str(raw["first_name"]),
            last_name=str(raw.get("last_name") or ""),
            vcard=str(raw.get("vcard") or ""),
        )
    except KeyError as exc:
        raise ContentError("контакт содержит неполные данные") from exc


def _validate_contact(contact: StoredContact) -> None:
    digits = contact.phone_number.removeprefix("+")
    if not digits.isdigit() or not 7 <= len(digits) <= 15:
        raise ContentError("номер контакта должен содержать от 7 до 15 цифр")
    if not 1 <= len(contact.first_name) <= 64:
        raise ContentError("имя контакта должно содержать от 1 до 64 символов")
    if len(contact.last_name) > 64:
        raise ContentError("фамилия контакта должна быть не длиннее 64 символов")
    if any(ord(char) < 32 for char in contact.first_name + contact.last_name):
        raise ContentError("имя контакта содержит недопустимые символы")
    if len(contact.vcard.encode("utf-8")) > 2048:
        raise ContentError("визитная карточка контакта слишком большая")


def store_media(
    settings: Settings,
    data: bytes,
    *,
    kind: str,
    original_name: str,
) -> StoredMedia:
    if kind not in ALLOWED_MEDIA_KINDS:
        raise ContentError("поддерживаются фото, видео, документы, голосовые и видеокружки")
    if not data:
        raise ContentError("медиафайл пуст")
    if len(data) > settings.media_max_bytes:
        raise ContentError(
            f"медиафайл больше лимита {settings.media_max_bytes // (1024 * 1024)} МБ"
        )

    clean_name = Path(original_name).name.strip() or f"{kind}.bin"
    suffix = Path(clean_name).suffix[:16].lower()
    root = Path(settings.media_storage_path).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    destination = root / f"{uuid4().hex}{suffix}"
    temporary = root / f".{destination.name}.tmp"
    try:
        temporary.write_bytes(data)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return StoredMedia(
        path=str(destination),
        kind=kind,
        name=clean_name[:255],
        size=len(data),
    )


def resolve_media_path(settings: Settings, raw_path: str) -> Path:
    root = Path(settings.media_storage_path).expanduser().resolve()
    path = Path(raw_path).expanduser().resolve()
    if not path.is_relative_to(root):
        raise ContentError("медиафайл находится вне защищённого хранилища")
    if not path.is_file():
        raise ContentError("медиафайл не найден")
    if path.stat().st_size > settings.media_max_bytes:
        raise ContentError("медиафайл превышает допустимый размер")
    return path
