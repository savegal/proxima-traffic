from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from proxima_traffic.bot import handlers
from proxima_traffic.models import ProxyEndpoint, ProxyProtocol, ProxyStatus
from proxima_traffic.proxies import parse_proxy, proxy_import_values

SECRET = "dd0123456789abcdef0123456789abcdef"
LINK = f"https://t.me/proxy?server=example.org&port=443&secret={SECRET}"


class State:
    def __init__(self):
        self.data = {"origin_chat_id": 1, "origin_message_id": 2}
        self.state = None

    async def get_data(self):
        return self.data.copy()

    async def update_data(self, **kwargs):
        self.data.update(kwargs)

    async def set_state(self, value):
        self.state = value

    async def get_state(self):
        return self.state

    async def clear(self):
        self.data.clear()
        self.state = None


@pytest.mark.parametrize("content", [
    LINK,
    f"Connect: {LINK}",
    f"[Подключить]({LINK.replace('&', r'\&')})",
    f"[{LINK}]({LINK})",
    f"`{LINK}`",
    LINK.replace("&", "&amp;"),
])
def test_extract_pasted_telegram_proxy_link(content):
    values = proxy_import_values(content)
    assert len(values) == 1
    parsed = parse_proxy(values[0])
    assert parsed.host == "example.org"
    assert parsed.password == SECRET
    assert parsed.protocol == ProxyProtocol.MTPROTO


def test_extract_preserves_real_duplicates_and_ordinary_txt_formats():
    values = proxy_import_values(
        f"# comment\n\n{LINK}\n{LINK}\nsocks5://user:password@proxy.example:1080"
    )
    assert len(values) == 3
    assert values[:2] == [LINK, LINK]
    assert parse_proxy(values[-1]).protocol == ProxyProtocol.SOCKS5


def test_extracts_forwarded_connect_button_urls():
    assert proxy_import_values("Connect", linked_urls=[LINK]) == [LINK]
    assert proxy_import_values(
        "Server: Unknown\nPort:22\nSecret: hidden\nConnect", linked_urls=[LINK]
    ) == [LINK]


async def test_mtproto_menu_accepts_text_in_one_step(monkeypatch):
    state = State()
    callback = SimpleNamespace(data="proxy:mtproto", answer=AsyncMock(), message=None)
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    await handlers.begin_proxy_import(callback, state)
    assert state.data["proxy_group"] == "MTProto"
    assert "прямо сообщением" in edit.call_args.args[1]


@pytest.mark.parametrize("hidden", [False, True])
async def test_text_import_saves_encrypted_proxy_without_downloading_a_file(
    app, monkeypatch, hidden
):
    state = State()
    message = SimpleNamespace(
        document=None, text="Connect" if hidden else f"{LINK}\n{LINK}", caption=None,
        entities=[SimpleNamespace(url=LINK)] if hidden else [], caption_entities=[],
        answer=AsyncMock(),
    )
    bot = SimpleNamespace(download=AsyncMock())
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_origin_data", edit)
    await handlers.import_proxy_document(message, state, bot, app.state.proxy_service)
    bot.download.assert_not_awaited()
    message.answer.assert_not_awaited()
    async with app.state.database.session_factory() as session:
        proxy = await session.scalar(select(ProxyEndpoint))
        assert proxy is not None
        assert proxy.host == "example.org"
        assert proxy.status == ProxyStatus.UNKNOWN
        assert SECRET not in proxy.password_encrypted
        assert app.state.proxy_service._cipher.decrypt(proxy.password_encrypted) == SECRET
    assert SECRET not in edit.call_args.args[2]
    if not hidden:
        assert "дубликатов <b>1</b>" in edit.call_args.args[2]
    assert state.data == {}


async def test_large_pasted_import_is_rejected_without_db_write(app):
    message = SimpleNamespace(
        document=None, text="\n".join([LINK] * 2001), caption=None,
        entities=[], caption_entities=[],
        answer=AsyncMock(),
    )
    state = State()
    state.data.clear()  # Exercise panel recovery when the original screen is unavailable.
    await handlers.import_proxy_document(message, state, object(), app.state.proxy_service)
    assert "2000" in message.answer.call_args.args[0]
    async with app.state.database.session_factory() as session:
        assert await session.scalar(select(ProxyEndpoint)) is None


async def test_txt_import_still_accepts_standard_proxy_lines(app, monkeypatch):
    content = f"{LINK}\nsocks5://proxy.example:1080\n"

    async def download(document, destination):
        destination.write(content.encode())
        destination.seek(0)

    message = SimpleNamespace(
        document=SimpleNamespace(file_name="proxies.txt", file_size=None), answer=AsyncMock(),
    )
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_origin_data", edit)
    await handlers.import_proxy_document(
        message, State(), SimpleNamespace(download=download), app.state.proxy_service
    )
    message.answer.assert_not_awaited()
    async with app.state.database.session_factory() as session:
        assert len(list(await session.scalars(select(ProxyEndpoint)))) == 2
