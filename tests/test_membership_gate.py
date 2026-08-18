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
