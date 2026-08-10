from proxima_traffic.failure_policy import failure_hint, failure_label


def test_operator_errors_have_readable_labels_and_actionable_hints() -> None:
    assert failure_label("reaction_unavailable") == (
        "реакции для сообщения или аккаунта недоступны"
    )
    assert "только реакция" in (failure_hint("reaction_unavailable") or "")
    assert failure_label("proxy_unavailable") == "назначенный прокси недоступен"
    assert "Проверьте" in (failure_hint("proxy_unavailable") or "")


def test_unknown_error_code_is_still_readable_and_has_no_invented_action() -> None:
    assert failure_label("future_gateway_code") == "future gateway code"
    assert failure_hint("future_gateway_code") is None
