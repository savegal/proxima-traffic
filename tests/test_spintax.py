from __future__ import annotations

import pytest

from proxima_traffic.content import ContentError, new_content_variant, render_spintax
from proxima_traffic.dialogues import validate_dialogue_steps
from proxima_traffic.services import CampaignRunner


def test_spintax_supports_nested_choices_and_stable_retries() -> None:
    template = "{Привет|Здравствуйте}, {друг|{коллега|приятель}}!"

    rendered = render_spintax(template, seed="run-101:primary")

    assert rendered in {
        "Привет, друг!",
        "Привет, коллега!",
        "Привет, приятель!",
        "Здравствуйте, друг!",
        "Здравствуйте, коллега!",
        "Здравствуйте, приятель!",
    }
    assert render_spintax(template, seed="run-101:primary") == rendered
    assert CampaignRunner._render_delivery_text(  # noqa: SLF001 - delivery invariant
        template,
        idempotency_key="run-101:primary",
    ) == rendered


def test_spintax_escape_syntax_preserves_literal_braces_and_pipe() -> None:
    rendered = render_spintax(r"\{цена\}: {10|20} | USD", seed="preview")

    assert rendered in {"{цена}: 10 | USD", "{цена}: 20 | USD"}


@pytest.mark.parametrize(
    "template",
    [
        "{единственный}",
        "{первый|второй",
        "первый}",
    ],
)
def test_spintax_rejects_ambiguous_or_malformed_templates(template: str) -> None:
    with pytest.raises(ContentError, match="Spintax"):
        render_spintax(template, seed="validation")


def test_spintax_is_validated_for_variants_and_dialogue_steps() -> None:
    with pytest.raises(ContentError, match="Spintax"):
        new_content_variant(name="Некорректный", text="{только один}")

    with pytest.raises(ValueError, match="Spintax"):
        validate_dialogue_steps(
            [
                {"account_id": 1, "text": "{только один}"},
                {"account_id": 2, "text": "Нормальный ответ"},
            ]
        )
