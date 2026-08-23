from proxima_traffic.membership_gate import (
    InlineButton,
    parse_membership_gate,
    telegram_channel_ref,
)


def test_subscription_gate_extracts_telegram_channels_and_safe_verify_callback() -> None:
    gate = parse_membership_gate(
        "Чтобы писать в чате, подпишитесь на каналы и нажмите Проверить.",
        (
            InlineButton(text="Канал 1", url="https://t.me/required_news"),
            InlineButton(text="Канал 2", url="tg://resolve?domain=required_chat"),
            InlineButton(text="Проверить", callback_data=b"verify-subscriptions"),
            InlineButton(text="Готово", callback_data=b"second-callback-is-not-clicked"),
            InlineButton(text="Открыть сайт", url="https://example.org"),
        ),
    )

    assert gate is not None
    assert gate.channel_refs == ("@required_news", "@required_chat")
    assert gate.verification_callbacks == (b"verify-subscriptions",)


def test_subscription_gate_recognises_membership_wording_beyond_subscribe() -> None:
    gate = parse_membership_gate(
        "Чтобы комментировать, станьте участником канала и подтвердите доступ.",
        (
            InlineButton(text="Вступить", url="https://t.me/required_news"),
            InlineButton(text="Подтвердить", callback_data=b"confirm-membership"),
        ),
    )

    assert gate is not None
    assert gate.channel_refs == ("@required_news",)
    assert gate.verification_callbacks == (b"confirm-membership",)


def test_subscription_gate_extracts_visible_text_links_handles_and_explicit_done_button() -> None:
    gate = parse_membership_gate(
        "Доступ к сообщениям будет после подписки: https://t.me/required_news, "
        "t.me/second_required и @third_required. Потом нажмите «Я подписался».",
        (InlineButton(text="✅ Я подписался", callback_data=b"subscription-complete"),),
    )

    assert gate is not None
    assert gate.channel_refs == ("@required_news", "@second_required", "@third_required")
    assert gate.verification_callbacks == (b"subscription-complete",)


def test_subscription_gate_extracts_hidden_telegram_text_url() -> None:
    gate = parse_membership_gate(
        "Щоб мати доступ до чату, підпишіться на канал і підтвердіть доступ.",
        (InlineButton(text="Підтвердити", callback_data=b"verify-access"),),
        text_links=("tg://resolve?domain=required_news",),
    )

    assert gate is not None
    assert gate.channel_refs == ("@required_news",)
    assert gate.verification_callbacks == (b"verify-access",)


def test_subscription_gate_recognises_common_russian_ukrainian_and_english_requirements() -> None:
    cases = (
        "Чтобы отправить сообщение, необходимо состоять в канале.",
        "Щоб писати, потрібно бути учасником каналу.",
        "To write here, you must be a member of the required channel.",
    )
    for notice in cases:
        gate = parse_membership_gate(
            notice,
            (InlineButton(text="Get access", callback_data=b"membership-done"),),
            text_links=("https://t.me/required_news",),
        )
        assert gate is not None
        assert gate.channel_refs == ("@required_news",)
        assert gate.verification_callbacks == (b"membership-done",)


def test_subscription_gate_rejects_captcha_and_web_app_buttons() -> None:
    assert (
        parse_membership_gate(
            "Чтобы писать, подпишитесь на канал и пройдите капчу.",
            (InlineButton(text="Канал", url="https://t.me/required_news"),),
        )
        is None
    )
    assert (
        parse_membership_gate(
            "Чтобы писать, подпишитесь на канал и подтвердите, что вы не бот.",
            (InlineButton(text="Канал", url="https://t.me/required_news"),),
        )
        is None
    )
    assert (
        parse_membership_gate(
            "To send messages you must subscribe.",
            (
                InlineButton(
                    text="Login and subscribe",
                    url="https://t.me/required_news",
                    is_web_app=True,
                ),
            ),
        )
        is None
    )
    assert (
        parse_membership_gate(
            "To send messages you must subscribe.",
            (
                InlineButton(
                    text="Subscribe",
                    url="https://t.me/required_news",
                    is_web_app=True,
                ),
            ),
        )
        is None
    )


def test_only_supported_telegram_channel_links_are_normalized() -> None:
    assert telegram_channel_ref("https://t.me/+privateInvite") == "https://t.me/+privateInvite"
    assert (
        telegram_channel_ref("https://t.me/joinchat/oldInvite") == "https://t.me/joinchat/oldInvite"
    )
    assert telegram_channel_ref("https://t.me/+privateInvite/extra") is None
    assert telegram_channel_ref("https://t.me/joinchat/oldInvite/extra") is None
    assert telegram_channel_ref("https://t.me/c/123/45") is None
    assert telegram_channel_ref("https://t.me/addlist/folder") is None
    assert telegram_channel_ref("https://example.org/required_news") is None
    assert telegram_channel_ref("t.me/required_news,") == "@required_news"
    assert telegram_channel_ref("https://t.me/not-a-valid-channel!") is None
    assert telegram_channel_ref("https://t.me/required_news/123") == "@required_news"
    assert telegram_channel_ref("https://t.me/s/required_news/123") == "@required_news"
    assert telegram_channel_ref("tg://join?invite=privateInvite") == "https://t.me/+privateInvite"
