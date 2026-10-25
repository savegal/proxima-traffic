from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Literal

NameLocale = Literal["mixed", "ru", "en"]


@dataclass(frozen=True, slots=True)
class ProfileName:
    first_name: str
    last_name: str
    locale: Literal["ru", "en"]
    style: Literal["first", "initial", "full"]

    @property
    def display_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()


_RU_GIVEN: dict[str, tuple[str, ...]] = {
    "f": (
        "Лера",
        "Соня",
        "Лиза",
        "Мила",
        "Милана",
        "Алиса",
        "Алина",
        "Даша",
        "Ксюша",
        "Настя",
        "Полина",
        "Вика",
        "Диана",
        "Карина",
        "Элина",
        "Ева",
        "Ника",
        "Арина",
        "Тася",
        "Яна",
        "Юля",
        "Аня",
        "Маша",
        "Катя",
        "Саша",
        "Лина",
        "Майя",
        "Амелия",
        "Василиса",
        "Злата",
        "Лея",
        "Марьяна",
        "Ульяна",
        "Кира",
        "Лада",
        "Рита",
        "Олеся",
        "Кристина",
        "Эмилия",
        "Стефания",
    ),
    "m": (
        "Макс",
        "Даня",
        "Никита",
        "Артём",
        "Егор",
        "Кирилл",
        "Илья",
        "Матвей",
        "Тимур",
        "Рома",
        "Влад",
        "Лёша",
        "Глеб",
        "Мирон",
        "Марк",
        "Лев",
        "Арсен",
        "Дамир",
        "Ярослав",
        "Федя",
        "Сева",
        "Женя",
        "Саша",
        "Миша",
        "Костя",
        "Стас",
        "Паша",
        "Андрей",
        "Антон",
        "Вадим",
        "Руслан",
        "Эмиль",
        "Давид",
        "Ринат",
        "Тёма",
        "Артур",
        "Денис",
        "Назар",
        "Роберт",
        "Самир",
    ),
}

_RU_SURNAMES: dict[str, tuple[str, ...]] = {
    "f": (
        "Морозова",
        "Волкова",
        "Лебедева",
        "Соколова",
        "Орлова",
        "Зайцева",
        "Крылова",
        "Миронова",
        "Романова",
        "Воронцова",
        "Данилова",
        "Максимова",
        "Никитина",
        "Фролова",
        "Королёва",
        "Вишневская",
        "Беляева",
        "Тихонова",
        "Жукова",
        "Павлова",
        "Ковалёва",
        "Белова",
        "Сафонова",
        "Новикова",
        "Власова",
        "Громова",
        "Ларионова",
        "Чернова",
        "Осипова",
        "Баранова",
        "Тарасова",
        "Мельникова",
        "Калинина",
        "Селезнёва",
        "Рыбакова",
        "Комарова",
        "Виноградова",
        "Савельева",
        "Куликова",
        "Наумова",
    ),
    "m": (
        "Морозов",
        "Волков",
        "Лебедев",
        "Соколов",
        "Орлов",
        "Зайцев",
        "Крылов",
        "Миронов",
        "Романов",
        "Воронцов",
        "Данилов",
        "Максимов",
        "Никитин",
        "Фролов",
        "Королёв",
        "Вишневский",
        "Беляев",
        "Тихонов",
        "Жуков",
        "Павлов",
        "Ковалёв",
        "Белов",
        "Сафонов",
        "Новиков",
        "Власов",
        "Громов",
        "Ларионов",
        "Чернов",
        "Осипов",
        "Баранов",
        "Тарасов",
        "Мельников",
        "Калинин",
        "Селезнёв",
        "Рыбаков",
        "Комаров",
        "Виноградов",
        "Савельев",
        "Куликов",
        "Наумов",
    ),
}

_EN_GIVEN: dict[str, tuple[str, ...]] = {
    "f": (
        "Mia",
        "Sofia",
        "Emma",
        "Eva",
        "Alice",
        "Alina",
        "Polina",
        "Lera",
        "Dasha",
        "Nika",
        "Mila",
        "Lina",
        "Maya",
        "Arina",
        "Karina",
        "Diana",
        "Kira",
        "Anya",
        "Lisa",
        "Kate",
        "Masha",
        "Sasha",
        "Vika",
        "Emily",
        "Chloe",
        "Olivia",
        "Amelia",
        "Ellie",
        "Lily",
        "Zoe",
        "Luna",
        "Nora",
        "Ivy",
        "Bella",
        "Lia",
        "Stella",
        "Tessa",
        "Nina",
        "Lana",
        "Mira",
        "Ava",
        "Leah",
        "Ruby",
        "Elisa",
        "Tina",
    ),
    "m": (
        "Max",
        "Leo",
        "Alex",
        "Dan",
        "Nick",
        "Mark",
        "Tim",
        "Artem",
        "Kirill",
        "Ilya",
        "Matvey",
        "Egor",
        "Roman",
        "Vlad",
        "Misha",
        "David",
        "Emil",
        "Amir",
        "Ryan",
        "Liam",
        "Noah",
        "Lucas",
        "Theo",
        "Adam",
        "Eric",
        "Ben",
        "Sam",
        "Milo",
        "Dylan",
        "Oscar",
        "Logan",
        "Evan",
        "Finn",
        "Nate",
        "Kai",
        "Dean",
        "Luke",
        "Chris",
        "Tony",
        "Robin",
        "Felix",
        "Aaron",
        "Jason",
        "Mike",
        "Rayan",
    ),
}

_EN_SURNAMES: tuple[str, ...] = (
    "Adams",
    "Baker",
    "Carter",
    "Collins",
    "Cooper",
    "Davis",
    "Evans",
    "Foster",
    "Gray",
    "Hall",
    "Harris",
    "Hayes",
    "Hill",
    "Kelly",
    "King",
    "Lee",
    "Lewis",
    "Martin",
    "Miller",
    "Moore",
    "Morgan",
    "Parker",
    "Reed",
    "Riley",
    "Ross",
    "Scott",
    "Taylor",
    "Walker",
    "White",
    "Young",
    "Woods",
    "Stone",
    "Brooks",
    "Bell",
    "West",
    "Lane",
    "Cole",
    "Price",
    "Ward",
    "Blake",
    "Clark",
    "Green",
    "James",
    "Turner",
    "Wilson",
)


def generate_profile_names(
    count: int,
    *,
    locale: NameLocale = "mixed",
    used_names: set[str] | None = None,
    rng: random.Random | None = None,
) -> list[ProfileName]:
    if count < 0:
        raise ValueError("count must not be negative")
    if locale not in {"mixed", "ru", "en"}:
        raise ValueError("unknown name locale")
    if count == 0:
        return []

    generator = rng or random.SystemRandom()
    excluded = {_name_key(value) for value in (used_names or set()) if value.strip()}
    pools = _candidate_pools(locale)
    for pool in pools.values():
        generator.shuffle(pool)

    drawn_styles = generator.choices(
        ("first", "initial", "full"),
        weights=(45, 15, 40),
        k=count,
    )
    quotas = {style: drawn_styles.count(style) for style in ("first", "initial", "full")}
    selected: list[ProfileName] = []
    selected_keys: set[str] = set()
    for style in ("first", "initial", "full"):
        remaining = quotas[style]
        if remaining == 0:
            continue
        for candidate in pools[style]:
            key = _name_key(candidate.display_name)
            if key in excluded or key in selected_keys:
                continue
            selected.append(candidate)
            selected_keys.add(key)
            remaining -= 1
            if remaining == 0:
                break

    if len(selected) < count:
        leftovers = [candidate for pool in pools.values() for candidate in pool]
        generator.shuffle(leftovers)
        for candidate in leftovers:
            key = _name_key(candidate.display_name)
            if key in excluded or key in selected_keys:
                continue
            selected.append(candidate)
            selected_keys.add(key)
            if len(selected) == count:
                break

    if len(selected) != count:
        raise ValueError("not enough unused profile names in the catalogue")
    generator.shuffle(selected)
    return selected


def profile_name_capacity(locale: NameLocale = "mixed") -> int:
    pools = _candidate_pools(locale)
    return len({_name_key(item.display_name) for pool in pools.values() for item in pool})


def available_profile_name_capacity(
    locale: NameLocale = "mixed", *, used_names: set[str] | None = None
) -> int:
    """Return the number of catalogue names not already used by an account."""
    excluded = {_name_key(value) for value in (used_names or set()) if value.strip()}
    pools = _candidate_pools(locale)
    return len(
        {
            _name_key(item.display_name)
            for pool in pools.values()
            for item in pool
            if _name_key(item.display_name) not in excluded
        }
    )


def _candidate_pools(locale: NameLocale) -> dict[str, list[ProfileName]]:
    pools: dict[str, list[ProfileName]] = {"first": [], "initial": [], "full": []}
    locales = ("ru", "en") if locale == "mixed" else (locale,)
    for current in locales:
        given_map = _RU_GIVEN if current == "ru" else _EN_GIVEN
        for gender, given_names in given_map.items():
            surnames = _RU_SURNAMES[gender] if current == "ru" else _EN_SURNAMES
            for first_name in given_names:
                pools["first"].append(ProfileName(first_name, "", current, "first"))
                seen_initials: set[str] = set()
                for last_name in surnames:
                    pools["full"].append(ProfileName(first_name, last_name, current, "full"))
                    initial = f"{last_name[0]}."
                    if initial not in seen_initials:
                        pools["initial"].append(
                            ProfileName(first_name, initial, current, "initial")
                        )
                        seen_initials.add(initial)
    return pools


def _name_key(value: str) -> str:
    return " ".join(value.split()).casefold()
