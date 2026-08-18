from proxima_traffic.bot.keyboards import (
    account_about_bulk_result_menu,
    account_browse_data,
    account_bulk_avatar_confirmation,
    account_bulk_avatar_result_menu,
    account_bulk_avatar_upload_menu,
    account_filters_menu,
    account_generated_names_confirmation,
    account_group_delete_confirmation,
    account_group_details_menu,
    account_group_members_menu,
    account_groups_menu,
    account_membership_menu,
    account_name_catalog_menu,
    account_profile_scope_menu,
    accounts_menu,
)
from proxima_traffic.bot.renderers import (
    account_generated_names_preview_text,
    account_name_catalog_text,
    account_profile_scope_text,
)
from proxima_traffic.bot.repository import AccountGroupView, AccountListFilter, Page
from proxima_traffic.models import Account, AccountStatus


def _callbacks(markup: object) -> set[str]:
    return {
        button.callback_data
        for row in markup.inline_keyboard  # type: ignore[attr-defined]
        for button in row
        if button.callback_data
    }


def test_account_section_keeps_filter_and_group_scope_in_navigation() -> None:
    account = Account(
        id=42,
        label="Premium reactor",
        ownership_confirmed=True,
        status=AccountStatus.ACTIVE,
        is_premium=True,
    )
    page = Page(items=[account], page=0, page_size=7, total=1)
    groups = [
        AccountGroupView(
            id=8,
            name="Auto reactions",
            description="Selected reaction pool",
            account_count=1,
            ready_count=1,
            premium_count=1,
        )
    ]
    scope_callback = account_browse_data(AccountListFilter.PREMIUM, 8, 0)

    account_callbacks = _callbacks(
        accounts_menu(page, account_filter=AccountListFilter.PREMIUM, group_id=8)
    )
    filter_callbacks = _callbacks(
        account_filters_menu(
            selected_filter=AccountListFilter.PREMIUM,
            group_id=8,
            page=0,
        )
    )
    group_callbacks = _callbacks(
        account_groups_menu(
            groups,
            account_filter=AccountListFilter.PREMIUM,
            group_id=8,
            page=0,
        )
    )

    assert scope_callback in account_callbacks
    assert "account:view:42:p:8:0" in account_callbacks
    assert scope_callback in filter_callbacks
    assert "account:group:view:8:p:8:0" in group_callbacks
    assert "account:group:add:list:p:8:0" in group_callbacks
    assert all(len(callback.encode()) <= 64 for callback in account_callbacks | group_callbacks)


def test_account_menu_visually_separates_navigation_checks_profile_and_import() -> None:
    page = Page(
        items=[Account(id=42, label="Prepared", ownership_confirmed=True)],
        page=0,
        page_size=7,
        total=1,
    )
    menu = accounts_menu(page)
    rows = [[button.text for button in row] for row in menu.inline_keyboard]

    assert ["── Навигация ──"] in rows
    assert ["── Проверка ──"] in rows
    assert ["── Оформление профиля ──"] in rows
    assert ["── Добавление ──"] in rows
    assert ["🎛 Все", "🗂 Группы"] in rows
    assert ["🎲 Имена", "📝 Описания"] in rows
    assert ["📥 Импорт", "➕ Без сессии"] in rows


def test_account_group_member_picker_and_details_have_safe_callbacks() -> None:
    accounts = [
        Account(
            id=7,
            label="Selected",
            ownership_confirmed=True,
            status=AccountStatus.ACTIVE,
        ),
        Account(
            id=9,
            label="Not selected",
            ownership_confirmed=True,
            status=AccountStatus.ACTIVE,
        ),
    ]
    page = Page(items=accounts, page=0, page_size=7, total=2)
    member_callbacks = _callbacks(
        account_group_members_menu(
            3,
            page,
            {7},
            account_filter=AccountListFilter.ALL,
        )
    )
    detail_callbacks = _callbacks(
        account_group_details_menu(
            3,
            account_filter=AccountListFilter.ALL,
            group_scope_id=None,
            page=0,
        )
    )

    assert "account:group:member:toggle:3:7:a:0" in member_callbacks
    assert "account:group:member:page:on:3:a:0" in member_callbacks
    assert "account:group:members:3:a:0" in detail_callbacks
    assert "account:group:edit:3:a:all:0" in detail_callbacks
    assert "account:group:delete:ask:3:a:all:0" in detail_callbacks
    assert all(len(callback.encode()) <= 64 for callback in member_callbacks | detail_callbacks)


def test_group_creation_from_an_account_keeps_context_and_selects_that_account() -> None:
    account = Account(
        id=7,
        label="Selected",
        ownership_confirmed=True,
        status=AccountStatus.ACTIVE,
    )
    page = Page(items=[account], page=3, page_size=7, total=29)
    group = AccountGroupView(
        id=9,
        name="Reactors",
        description="",
        account_count=1,
        ready_count=1,
        premium_count=0,
    )
    callbacks = _callbacks(
        account_membership_menu(
            account.id,
            [group],
            set(),
            account_filter=AccountListFilter.READY,
            group_scope_id=9,
            page=page.page,
        )
    )

    assert "account:group:add:member:7:r:9:3" in callbacks
    assert all(len(callback.encode()) <= 64 for callback in callbacks)


def test_account_section_callback_payloads_fit_telegram_limit_at_large_ids() -> None:
    largest_id = 2_147_483_647
    account = Account(
        id=largest_id,
        label="Large ID",
        ownership_confirmed=True,
        status=AccountStatus.ACTIVE,
    )
    group = AccountGroupView(
        id=largest_id,
        name="Large group",
        description="",
        account_count=1,
        ready_count=0,
        premium_count=0,
    )
    page = Page(items=[account], page=largest_id, page_size=7, total=largest_id)
    markups = [
        accounts_menu(
            page,
            account_filter=AccountListFilter.NEEDS_ATTENTION,
            group_id=largest_id,
        ),
        account_group_delete_confirmation(
            largest_id,
            account_filter=AccountListFilter.NEEDS_ATTENTION,
            group_scope_id=largest_id,
            page=largest_id,
        ),
        account_group_details_menu(
            largest_id,
            account_filter=AccountListFilter.NEEDS_ATTENTION,
            group_scope_id=largest_id,
            page=largest_id,
        ),
        account_group_members_menu(
            largest_id,
            page,
            {largest_id},
            account_filter=AccountListFilter.NEEDS_ATTENTION,
        ),
        account_membership_menu(
            largest_id,
            [group],
            {largest_id},
            account_filter=AccountListFilter.NEEDS_ATTENTION,
            group_scope_id=largest_id,
            page=largest_id,
        ),
    ]
    callbacks = {callback for markup in markups for callback in _callbacks(markup)}

    assert len(account_browse_data(AccountListFilter.NEEDS_ATTENTION, largest_id, largest_id)) <= 64
    assert all(len(callback.encode()) <= 64 for callback in callbacks)


def test_bulk_profile_scope_ui_limits_changes_to_ready_accounts_and_preserves_navigation() -> None:
    groups = [
        AccountGroupView(
            id=8,
            name="Новая поставка августа",
            description="",
            account_count=9,
            ready_count=7,
            premium_count=2,
        ),
        AccountGroupView(
            id=9,
            name="Требуют проверки",
            description="",
            account_count=4,
            ready_count=0,
            premium_count=0,
        ),
    ]
    scope_menu = account_profile_scope_menu(
        groups,
        operation="names",
        ready_count=18,
        back_callback="menu:accounts:3",
    )
    scope_callbacks = _callbacks(scope_menu)
    scope_text = account_profile_scope_text(
        groups,
        operation_label="имён Telegram",
        ready_count=18,
    )
    catalog = account_name_catalog_text([], scope_label="Новая поставка августа")
    catalog_menu = account_name_catalog_menu(
        scope_label="Новая поставка августа",
        back_callback="account:profile:scope:names:all",
    )
    preview = account_generated_names_preview_text(
        [
            {
                "account_id": 7,
                "label": "купленный аккаунт",
                "first_name": "Лера",
                "last_name": "М.",
            }
        ],
        locale="mixed",
        scope_label="Новая поставка августа",
    )
    confirmation = account_generated_names_confirmation(
        locale="mixed",
        account_id=0,
        page=0,
        scope_label="Новая поставка августа",
        back_callback="account:profile:scope:names:all",
        retry_callback="account:names:preview:mixed:0:0",
    )

    assert "account:profile:scope:names:all" in scope_callbacks
    assert "account:profile:scope:names:group:8" in scope_callbacks
    assert "account:profile:scope:names:group:9" not in scope_callbacks
    assert "menu:accounts:3" in scope_callbacks
    assert "Все готовые:</b> 18" in scope_text
    assert "Новая поставка августа</b> · готово 7 из 9" in scope_text
    assert "Требуют проверки" not in scope_text
    assert "ОБЛАСТЬ · Новая поставка августа · ДОСТУПНО 0 АККАУНТОВ" in catalog
    assert "СМЕШАННЫЕ · Новая поставка августа · АККАУНТОВ 1" in preview
    assert "account:profile:scope:names:all" in _callbacks(catalog_menu)
    assert "account:names:preview:mixed:0:0" in _callbacks(confirmation)
    assert "account:profile:scope:names:all" in _callbacks(confirmation)
    assert all(
        len(callback.encode()) <= 64
        for callback in scope_callbacks | _callbacks(catalog_menu) | _callbacks(confirmation)
    )


def test_bulk_avatar_ui_uses_scope_then_an_exact_confirmation() -> None:
    account = Account(
        id=42,
        label="Ready account",
        ownership_confirmed=True,
        status=AccountStatus.ACTIVE,
    )
    page = Page(items=[account], page=0, page_size=7, total=1)
    groups = [
        AccountGroupView(
            id=8,
            name="New delivery",
            description="",
            account_count=3,
            ready_count=3,
            premium_count=0,
        )
    ]

    accounts_callbacks = _callbacks(accounts_menu(page))
    scope_callbacks = _callbacks(
        account_profile_scope_menu(groups, operation="avatar", ready_count=3)
    )
    upload_callbacks = _callbacks(account_bulk_avatar_upload_menu())
    confirmation = account_bulk_avatar_confirmation(count=3)
    confirmation_callbacks = _callbacks(confirmation)
    result_callbacks = _callbacks(account_bulk_avatar_result_menu())

    assert "account:avatars:bulk" in accounts_callbacks
    assert "account:profile:scope:avatar:all" in scope_callbacks
    assert "account:profile:scope:avatar:group:8" in scope_callbacks
    assert "account:avatars:bulk:cancel" in upload_callbacks
    assert "account:avatars:bulk:apply" in confirmation_callbacks
    assert "account:avatars:bulk:cancel" in confirmation_callbacks
    assert "3" in confirmation.inline_keyboard[0][0].text
    assert "account:avatars:bulk" in result_callbacks
    assert all(
        len(callback.encode()) <= 64
        for callback in (
            accounts_callbacks
            | scope_callbacks
            | upload_callbacks
            | confirmation_callbacks
            | result_callbacks
        )
    )


def test_completed_bulk_about_has_no_stale_destructive_callbacks() -> None:
    callbacks = _callbacks(account_about_bulk_result_menu())

    assert "account:about:bulk:set" not in callbacks
    assert "account:about:bulk:clear:ask" not in callbacks
    assert callbacks == {"account:about:bulk", "menu:accounts:0"}
