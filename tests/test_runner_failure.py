import httpx

from proxima_traffic.config import Settings
from proxima_traffic.gateways import SentMessage
from proxima_traffic.main import create_app


class FailOnReplyGateway:
    def __init__(self) -> None:
        self.calls = 0

    async def send_message(self, **kwargs: object) -> SentMessage:
        self.calls += 1
        if self.calls == 2:
            raise RuntimeError("simulated reply failure")
        return SentMessage(message_id=501)


async def test_reply_failure_preserves_primary_action_and_records_failure(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'failure.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        worker_max_attempts=2,
    )
    app = create_app(settings=settings, gateway=FailOnReplyGateway())
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {"X-Admin-Key": "test-admin-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        primary = await _create_account(client, headers, "failure-primary", 301)
        reply = await _create_account(client, headers, "failure-reply", 302)
        target = (
            await client.post(
                "/api/v1/targets",
                headers=headers,
                json={"chat_ref": "@failure_test", "title": "Failure test"},
            )
        ).json()
        await client.post(f"/api/v1/targets/{target['id']}/approve", headers=headers)
        campaign = (
            await client.post(
                "/api/v1/campaigns",
                headers=headers,
                json={
                    "name": "reply-failure",
                    "primary_account_id": primary["id"],
                    "reply_account_id": reply["id"],
                    "target_id": target["id"],
                    "primary_text": "First",
                    "reply_text": "Reply",
                },
            )
        ).json()

        queued = await client.post(
            f"/api/v1/campaigns/{campaign['id']}/runs",
            headers=headers,
            json={"request_key": "failure-request-001"},
        )
        assert queued.status_code == 202
        first_step = await app.state.queue_worker.process_next()
        assert first_step.status.value == "queued"
        failed_step = await app.state.queue_worker.process_next()
        assert failed_step.status.value == "failed"

        status_response = await client.get("/api/v1/status", headers=headers)
        assert status_response.json()["runs"] == 1
        details = await client.get("/api/v1/runs/1", headers=headers)
        body = details.json()
        assert body["status"] == "failed"
        assert body["primary_message_id"] == 501
        assert body["reply_message_id"] is None
        assert [action["status"] for action in body["actions"]] == ["succeeded", "failed"]
        assert body["actions"][1]["reply_to_message_id"] == 501

    await app.state.database.dispose()


async def _create_account(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    label: str,
    telegram_user_id: int,
) -> dict[str, object]:
    response = await client.post(
        "/api/v1/accounts",
        headers=headers,
        json={
            "label": label,
            "telegram_user_id": telegram_user_id,
            "ownership_confirmed": True,
        },
    )
    assert response.status_code == 201
    return response.json()
