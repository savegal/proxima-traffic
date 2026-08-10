import pytest

from proxima_traffic.target_import import (
    TargetReferenceError,
    TargetReferenceKind,
    parse_target_batch,
    parse_target_reference,
)


@pytest.mark.parametrize(
    ("source", "canonical", "kind"),
    [
        ("@My_Channel", "@my_channel", TargetReferenceKind.PUBLIC_USERNAME),
        ("https://t.me/My_Channel", "@my_channel", TargetReferenceKind.PUBLIC_USERNAME),
        ("https://t.me/s/My_Channel/42", "@my_channel", TargetReferenceKind.PUBLIC_USERNAME),
        ("tg://resolve?domain=My_Channel", "@my_channel", TargetReferenceKind.PUBLIC_USERNAME),
        ("-1001234567890", "-1001234567890", TargetReferenceKind.NUMERIC_CHAT_ID),
        (
            "https://t.me/+AbCdEf_12345",
            "https://t.me/+AbCdEf_12345",
            TargetReferenceKind.PRIVATE_INVITE,
        ),
        (
            "https://t.me/joinchat/AbCdEf_12345",
            "https://t.me/+AbCdEf_12345",
            TargetReferenceKind.PRIVATE_INVITE,
        ),
        (
            "https://t.me/addlist/Q6_wAscuSB1mYTBk",
            "https://t.me/addlist/Q6_wAscuSB1mYTBk",
            TargetReferenceKind.CHAT_FOLDER,
        ),
    ],
)
def test_smart_target_parser(source: str, canonical: str, kind: TargetReferenceKind) -> None:
    parsed = parse_target_reference(source)
    assert parsed.canonical == canonical
    assert parsed.kind == kind


def test_target_batch_deduplicates_and_separates_folder_links() -> None:
    result = parse_target_batch(
        [
            "# comment",
            "@My_Channel",
            "https://t.me/my_channel/100",
            "https://t.me/+InviteHash_123",
            "https://t.me/addlist/Q6_wAscuSB1mYTBk",
            "https://example.com/not-telegram",
            "",
        ]
    )
    assert result.canonical_targets == ["@my_channel", "https://t.me/+InviteHash_123"]
    assert result.duplicates == 1
    assert result.ignored == 2
    assert len(result.folders) == 1
    assert result.folders[0].slug == "Q6_wAscuSB1mYTBk"
    assert len(result.issues) == 1
    assert result.issues[0].line == 6


def test_invite_and_folder_hashes_remain_case_sensitive() -> None:
    result = parse_target_batch(
        [
            "https://t.me/+InviteHash_123",
            "https://t.me/+invitehash_123",
            "https://t.me/addlist/FolderHash_123",
            "https://t.me/addlist/folderhash_123",
        ]
    )
    assert len(result.targets) == 2
    assert len(result.folders) == 2
    assert result.duplicates == 0


@pytest.mark.parametrize(
    "value",
    [
        "https://example.com/channel",
        "https://t.me/share/url?url=test",
        "https://t.me/c/123/456",
        "not a username",
    ],
)
def test_target_parser_rejects_non_chat_links(value: str) -> None:
    with pytest.raises(TargetReferenceError):
        parse_target_reference(value)


def test_target_parser_rejects_positive_numeric_user_id() -> None:
    with pytest.raises(TargetReferenceError, match="отрицательным"):
        parse_target_reference("123456789")
