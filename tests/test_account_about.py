from types import SimpleNamespace

import pytest
from sqlalchemy import select

from proxima_traffic.models import Account


@pytest.mark.asyncio
async def test_account_profile_about_is_sent_saved_and_cleared(app, monkeypatch) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(label="about-account", authorized=True, ownership_confirmed=True)
        session.add(account)
        await session.commit()
        account_id = int(account.id)

    requests: list[object] = []

    async def fake_operation(profile_account_id: int, operation):
        assert profile_account_id == account_id

        class Client:
            async def __call__(self, request: object) -> object:
                requests.append(request)
                return SimpleNamespace()

        await operation(Client())
        return SimpleNamespace(first_name="Alex", last_name=None, username="alex", photo=None), None

    monkeypatch.setattr(app.state.account_service, "_run_profile_operation", fake_operation)

    updated = await app.state.account_service.update_profile_about(
        account_id, about="https://example.com"
    )
    assert updated.about == "https://example.com"
    assert requests[-1].about == "https://example.com"

    cleared = await app.state.account_service.update_profile_about(account_id, about="")
    assert cleared.about == ""
    assert requests[-1].about == ""

    async with app.state.database.session_factory() as session:
        stored = await session.scalar(select(Account).where(Account.id == account_id))
        assert stored is not None
        assert stored.about == ""
