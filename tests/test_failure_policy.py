from proxima_traffic.failure_policy import failure_hint, failure_label


def test_operator_errors_have_readable_labels_and_actionable_hints() -> None:
    assert failure_label("album_duplicate") == "элемент альбома обработан общим комментарием"
    assert failure_label("reaction_unavailable") == (
        "выбранная реакция запрещена в этом сообщении или недоступна аккаунту"
    )
    assert "выбранный Emoji" in (failure_hint("reaction_unavailable") or "")
    assert failure_label("reaction_delivery_unconfirmed") == (
        "Telegram принял реакцию, но не подтвердил её отображение"
    )
    assert failure_label("sender_unavailable") == "назначенный аккаунт недоступен"
    assert "сессия недействительна" in (failure_hint("sender_unavailable") or "")
    assert failure_label("proxy_unavailable") == "не найден рабочий прокси для аккаунта"
    assert "Автозамена" in (failure_hint("proxy_unavailable") or "")
    assert failure_label("capacity_window_exceeded") == (
        "аккаунт не успевает отправить до конца active-окна"
    )
    assert "Увеличьте срок active-рассылки" in (
        failure_hint("capacity_window_exceeded") or ""
    )


def test_unknown_error_code_is_still_readable_and_has_no_invented_action() -> None:
    assert failure_label("future_gateway_code") == "future gateway code"
    assert failure_hint("future_gateway_code") is None
