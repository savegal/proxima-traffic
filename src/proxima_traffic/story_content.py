"""Plain story captions, Telegram UTF-16 offsets and bounded reaction choices."""

from __future__ import annotations

import re
from io import BytesIO

from proxima_traffic.content import ContentError

STORY_REACTIONS = ("🔥", "❤️", "👍", "⚡", "🎉", "👏", "😍", "🥳", "💯", "⭐")


def prepare_story_photo(data: bytes) -> bytes:
    """Decode and prepare a vertical RGB JPEG, retaining the entire source image."""
    from PIL import Image, ImageOps, UnidentifiedImageError

    if not data or len(data) > 30 * 1024 * 1024:
        raise ContentError("Фото истории должно быть непустым и не больше 30 МБ")
    try:
        with Image.open(BytesIO(data)) as image:
            if image.format not in {"JPEG", "PNG"}:
                raise ContentError("Для истории отправьте изображение в формате JPG или PNG")
            if min(image.size) < 320:
                raise ContentError("Минимальная сторона изображения — 320 пикселей")
            if image.width * image.height > 40_000_000:
                raise ContentError(
                    "Разрешение изображения слишком большое: максимум 40 мегапикселей"
                )
            image.load()
            oriented = ImageOps.exif_transpose(image)
            fitted = ImageOps.contain(oriented, (1080, 1920), Image.Resampling.LANCZOS)
            canvas = Image.new("RGB", (1080, 1920), (0, 0, 0))
            xy = ((1080 - fitted.width) // 2, (1920 - fitted.height) // 2)
            if fitted.mode in {"RGBA", "LA"} or "transparency" in fitted.info:
                rgba = fitted.convert("RGBA")
                canvas.paste(rgba, xy, rgba.getchannel("A"))
            else:
                canvas.paste(fitted.convert("RGB"), xy)
            output = BytesIO()
            canvas.save(output, "JPEG", quality=90, optimize=True)
            return output.getvalue()
    except ContentError:
        raise
    except (OSError, ValueError, Image.DecompressionBombError, UnidentifiedImageError) as exc:
        raise ContentError(
            "Не удалось прочитать фото истории: файл повреждён или не поддерживается"
        ) from exc


def story_video_attributes(path: str):
    """Reject unreadable story videos instead of uploading Telethon's 1x1 fallback."""
    from telethon import types, utils

    try:
        attributes, mime_type = utils.get_attributes(path, supports_streaming=True)
    except Exception as exc:
        raise ContentError("Не удалось прочитать видео. Пересохраните его в MP4") from exc
    video = next((a for a in attributes if isinstance(a, types.DocumentAttributeVideo)), None)
    if mime_type != "video/mp4" or video is None or video.w <= 1 or video.h <= 1:
        raise ContentError("Не удалось прочитать видеодорожку MP4. Пересохраните видео")
    if not 0 <= video.duration <= 60:
        raise ContentError("Видео для истории должно быть не длиннее 60 секунд")
    return attributes, mime_type


def utf16_length(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def story_username(value: str) -> str:
    value = value.strip().lstrip("@")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,31}", value):
        raise ValueError("Некорректный username для отметки в истории")
    return value


def normalize_story_reactions(value: str) -> str:
    value = value.strip()
    if value.lower() == "random":
        return "random"
    choices = tuple(dict.fromkeys(part.strip() for part in value.split(",")))
    if not choices or any(part not in STORY_REACTIONS for part in choices):
        raise ValueError("Выберите реакцию кнопкой или укажите поддерживаемые эмодзи через запятую")
    return ",".join(choices)


def story_caption(caption: str, usernames, limit: int):
    """Return the literal caption and explicit mention entities; never drop tags."""
    from telethon.types import MessageEntityMention

    text = caption.strip()
    entities = []
    seen = set()
    for username in usernames:
        name = story_username(username)
        if name.casefold() in seen:
            raise ValueError("Повторяющийся username в истории")
        seen.add(name.casefold())
        separator = "\n\n" if not entities and text else " " if text else ""
        text += separator
        tag = "@" + name
        entities.append(MessageEntityMention(offset=utf16_length(text), length=utf16_length(tag)))
        text += tag
    if not entities:
        raise ValueError("Для теггинга нужна хотя бы одна отметка")
    if utf16_length(text) > limit:
        raise ValueError("Подпись и все отметки не помещаются в лимит истории; сократите подпись")
    return text, entities


def pack_story_mentions(rows, caption, max_tags, limit):
    chunks = []
    chunk = []
    seen = set()
    for row in rows:
        try:
            username = story_username(row[1])
        except ValueError:
            continue
        if username.casefold() in seen:
            continue
        if chunk:
            try:
                story_caption(caption, [r[1] for r in chunk] + [username], limit)
                fits = len(chunk) < max_tags
            except ValueError:
                fits = False
            if not fits:
                chunks.append(chunk)
                chunk = []
        story_caption(caption, [username], limit)
        chunk.append((row[0], username))
        seen.add(username.casefold())
    if chunk:
        chunks.append(chunk)
    if not chunks:
        raise ValueError("В базе нет подходящих username для отметки")
    return chunks
