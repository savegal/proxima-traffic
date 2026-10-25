import random

import pytest

from proxima_traffic.profile_names import (
    available_profile_name_capacity,
    generate_profile_names,
    profile_name_capacity,
)


def test_profile_name_catalog_has_large_ru_and_latin_reserve() -> None:
    assert profile_name_capacity("ru") > 4_000
    assert profile_name_capacity("en") > 5_000
    assert profile_name_capacity("mixed") > 10_000


def test_generated_profile_names_are_unique_natural_and_mixed() -> None:
    names = generate_profile_names(200, locale="mixed", rng=random.Random(2026))

    assert len(names) == 200
    assert len({item.display_name.casefold() for item in names}) == 200
    assert {item.locale for item in names} == {"ru", "en"}
    assert {item.style for item in names} == {"first", "initial", "full"}
    assert all(1 <= len(item.first_name) <= 64 for item in names)
    assert all(len(item.last_name) <= 64 for item in names)
    assert all(not any(character.isdigit() for character in item.display_name) for item in names)


def test_profile_name_generator_excludes_existing_names() -> None:
    baseline = generate_profile_names(20, locale="ru", rng=random.Random(7))
    used = {item.display_name for item in baseline}
    replacement = generate_profile_names(
        20,
        locale="ru",
        used_names=used,
        rng=random.Random(7),
    )

    assert not (
        {item.display_name.casefold() for item in replacement} & {x.casefold() for x in used}
    )


def test_available_profile_name_capacity_excludes_used_names() -> None:
    baseline = generate_profile_names(3, locale="en", rng=random.Random(11))

    assert available_profile_name_capacity(
        "en", used_names={item.display_name for item in baseline}
    ) == (profile_name_capacity("en") - 3)


def test_profile_name_generator_rejects_invalid_requests() -> None:
    with pytest.raises(ValueError, match="negative"):
        generate_profile_names(-1)
    with pytest.raises(ValueError, match="unknown"):
        generate_profile_names(1, locale="unknown")  # type: ignore[arg-type]


@pytest.mark.parametrize("locale", ["ru", "en", "mixed"])
@pytest.mark.parametrize("count", [1, 2, 3, 5, 10, 200])
def test_profile_names_match_requested_batch_size_for_small_and_large_selections(locale, count):
    for seed in range(10):
        names = generate_profile_names(count, locale=locale, rng=random.Random(seed))
        assert len(names) == count
        assert len({name.display_name.casefold() for name in names}) == count


@pytest.mark.parametrize("style", ["first", "initial", "full"])
def test_single_name_uses_only_the_selected_style(style):
    class SingleStyleRandom(random.Random):
        def choices(self, population, weights=None, *, cum_weights=None, k=1):
            return [style] * k

    names = generate_profile_names(1, locale="ru", rng=SingleStyleRandom(7))
    assert len(names) == 1 and names[0].style == style


def test_name_generation_falls_back_when_the_selected_style_has_no_free_names():
    class FirstNameRandom(random.Random):
        def choices(self, population, weights=None, *, cum_weights=None, k=1):
            return ["first"] * k

    used_names = set()
    while True:
        names = generate_profile_names(
            1, locale="ru", used_names=used_names, rng=FirstNameRandom(3)
        )
        if names[0].style != "first":
            break
        used_names.add(names[0].display_name)
    assert names[0].display_name not in used_names
    assert names[0].style in {"initial", "full"}
