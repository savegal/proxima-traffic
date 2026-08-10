import pytest

from proxima_traffic.content import (
    BroadcastContentVariant,
    ContentError,
    StoredMedia,
    new_content_variant,
    new_stored_contact,
    parse_content_variants,
    serialize_content_variants,
)
from proxima_traffic.dialogues import validate_dialogue_steps


def test_contact_variant_round_trip_normalizes_phone_and_preserves_card() -> None:
    contact = new_stored_contact(
        phone_number="+7 (999) 123-45-67",
        first_name="  Алиса  ",
        last_name="  Соколова  ",
    )
    variant = new_content_variant(name="Связаться", text="", contact=contact)

    restored = parse_content_variants(
        serialize_content_variants([variant]),
        fallback_text="unused",
    )[0]

    assert restored.contact is not None
    assert restored.contact.phone_number == "+79991234567"
    assert restored.contact.display_name == "Алиса Соколова"
    assert restored.text == ""
    assert restored.media == ()


@pytest.mark.parametrize(
    ("phone", "first_name", "match"),
    [
        ("123", "Алиса", "от 7 до 15"),
        ("+7999123456789012", "Алиса", "от 7 до 15"),
        ("call79991234567", "Алиса", "недопустимые символы"),
        ("+79991234567", "", "имя контакта"),
    ],
)
def test_invalid_contact_is_rejected(phone: str, first_name: str, match: str) -> None:
    with pytest.raises(ContentError, match=match):
        new_stored_contact(phone_number=phone, first_name=first_name)


def test_contact_cannot_be_mixed_with_text_or_media() -> None:
    contact = new_stored_contact(phone_number="+79991234567", first_name="Алиса")

    with pytest.raises(ContentError, match="отдельным вариантом"):
        new_content_variant(name="Wrong", text="Text", contact=contact)

    with pytest.raises(ContentError, match="отдельным вариантом"):
        serialize_content_variants(
            [
                BroadcastContentVariant(
                    variant_id="wrong",
                    name="Wrong",
                    text="",
                    html_text="<b>Wrong</b>",
                    contact=contact,
                )
            ]
        )


def test_contact_vcard_limit_is_measured_in_utf8_bytes() -> None:
    new_stored_contact(
        phone_number="+79991234567",
        first_name="Алиса",
        vcard="я" * 1024,
    )
    with pytest.raises(ContentError, match="слишком большая"):
        new_stored_contact(
            phone_number="+79991234567",
            first_name="Алиса",
            vcard="я" * 1025,
        )


def test_voice_can_be_sent_without_a_caption() -> None:
    voice = StoredMedia(path="voice.ogg", kind="voice", name="voice.ogg", size=512)

    variant = new_content_variant(name="Голосовое", text="", media=(voice,))

    assert variant.text == ""
    assert variant.media == (voice,)


@pytest.mark.parametrize("kind", ["photo", "video", "document"])
def test_single_media_can_be_sent_without_a_caption(kind: str) -> None:
    media = StoredMedia(path=f"item.{kind}", kind=kind, name=f"item.{kind}", size=512)

    variant = new_content_variant(name=f"Без подписи · {kind}", text="", media=(media,))

    assert variant.text == ""
    assert variant.media == (media,)


def test_video_note_never_accepts_a_caption_or_album() -> None:
    circle = StoredMedia(path="circle.mp4", kind="video_note", name="circle.mp4", size=512)
    photo = StoredMedia(path="photo.jpg", kind="photo", name="photo.jpg", size=512)

    with pytest.raises(ContentError, match="видеокружку"):
        new_content_variant(name="Кружок", text="Подпись", media=(circle,))
    with pytest.raises(ContentError, match="только по одному"):
        new_content_variant(name="Альбом", text="Текст", media=(circle, photo))


def test_dialogue_accepts_a_captionless_voice_and_rejects_a_captioned_circle() -> None:
    steps = validate_dialogue_steps(
        [
            {
                "account_id": 1,
                "text": "",
                "media_path": "voice.ogg",
                "media_kind": "voice",
                "reply_to_previous": False,
            },
            {"account_id": 2, "text": "Ответ", "delay_seconds": 10},
        ]
    )
    assert steps[0].media_kind == "voice"

    with pytest.raises(ValueError, match="видеокружку"):
        validate_dialogue_steps(
            [
                {
                    "account_id": 1,
                    "text": "Подпись",
                    "media_path": "circle.mp4",
                    "media_kind": "video_note",
                    "reply_to_previous": False,
                },
                {"account_id": 2, "text": "Ответ", "delay_seconds": 10},
            ]
        )


def test_dialogue_accepts_a_captionless_photo() -> None:
    steps = validate_dialogue_steps(
        [
            {
                "account_id": 1,
                "text": "",
                "media_path": "photo.jpg",
                "media_kind": "photo",
                "reply_to_previous": False,
            },
            {"account_id": 2, "text": "Ответ", "delay_seconds": 10},
        ]
    )

    assert steps[0].media_kind == "photo"


def test_dialogue_rejects_orphaned_media_metadata() -> None:
    with pytest.raises(ValueError, match="метаданные медиа указаны без файла"):
        validate_dialogue_steps(
            [
                {
                    "account_id": 1,
                    "text": "Первое",
                    "media_name": "voice.ogg",
                    "reply_to_previous": False,
                },
                {"account_id": 2, "text": "Ответ", "delay_seconds": 10},
            ]
        )
