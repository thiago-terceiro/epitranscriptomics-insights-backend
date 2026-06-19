"""
Unit tests for POST /jobs/pod5 API endpoint.

Tests the validation logic without hitting a real DB, Redis, or Celery.
All external dependencies are patched.
"""
import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient, ASGITransport


# ---------------------------------------------------------------------------
# Test client setup
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_upload_record():
    """A valid, completed upload session record."""
    return {
        "session_id": "sess-abc-123",
        "user_id": "anonymous",
        "completed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "files": [
            {"object_name": "uploads/anonymous/sess-abc-123/reads.pod5", "size": 1_000_000}
        ],
    }


@pytest.fixture()
def app_client(mock_upload_record: dict):
    """
    Return an httpx AsyncClient wired to the FastAPI app with all
    external calls patched out.
    """
    import importlib

    # Patch heavy startup I/O before importing the app
    patches = [
        patch("worker.api.db.session.init_db", new_callable=AsyncMock),
        patch("worker.api.main.redis_client", new_callable=AsyncMock),
    ]
    for p in patches:
        p.start()

    from worker.api.main import app

    yield app

    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Helper: build a session mock that returns given mapping
# ---------------------------------------------------------------------------

def _make_db_session(record: dict | None) -> MagicMock:
    """Build a mock AsyncSession whose execute() returns the given record."""
    mapping_result = MagicMock()
    mapping_result.first.return_value = record
    execute_result = MagicMock()
    execute_result.mappings.return_value = mapping_result

    session = MagicMock()
    session.execute = AsyncMock(return_value=execute_result)
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPod5Endpoint:
    BASE = "/jobs/pod5"
    VALID_BODY = {
        "upload_session_id": "sess-abc-123",
        "pod5_object_name": "uploads/anonymous/sess-abc-123/reads.pod5",
    }

    @pytest.mark.asyncio
    async def test_valid_request_returns_202(self, app_client, mock_upload_record):
        db_session = _make_db_session(mock_upload_record)

        with patch("worker.api.main.get_session", return_value=db_session), \
             patch("worker.api.main.celery_app") as mock_celery, \
             patch("worker.api.main.redis_client") as mock_redis:
            mock_redis.hset = AsyncMock()
            mock_redis.expire = AsyncMock()
            mock_celery.send_task = MagicMock(return_value=None)

            async with AsyncClient(
                transport=ASGITransport(app=app_client), base_url="http://test"
            ) as client:
                resp = await client.post(self.BASE, json=self.VALID_BODY)

        assert resp.status_code == 200
        data = resp.json()
        assert "job_id" in data
        assert data["status"] == "queued"

    @pytest.mark.asyncio
    async def test_unknown_session_returns_404(self, app_client):
        db_session = _make_db_session(None)  # not found

        with patch("worker.api.main.get_session", return_value=db_session):
            async with AsyncClient(
                transport=ASGITransport(app=app_client), base_url="http://test"
            ) as client:
                resp = await client.post(self.BASE, json=self.VALID_BODY)

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_incomplete_session_returns_400(self, app_client, mock_upload_record):
        incomplete = {**mock_upload_record, "completed_at": None}
        db_session = _make_db_session(incomplete)

        with patch("worker.api.main.get_session", return_value=db_session):
            async with AsyncClient(
                transport=ASGITransport(app=app_client), base_url="http://test"
            ) as client:
                resp = await client.post(self.BASE, json=self.VALID_BODY)

        assert resp.status_code == 400
        assert "not completed" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_object_not_in_session_returns_400(self, app_client, mock_upload_record):
        body = {**self.VALID_BODY, "pod5_object_name": "uploads/anonymous/other-sess/evil.pod5"}
        db_session = _make_db_session(mock_upload_record)

        with patch("worker.api.main.get_session", return_value=db_session):
            async with AsyncClient(
                transport=ASGITransport(app=app_client), base_url="http://test"
            ) as client:
                resp = await client.post(self.BASE, json=body)

        assert resp.status_code == 400
        assert "does not belong" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_missing_required_fields_returns_422(self, app_client):
        async with AsyncClient(
            transport=ASGITransport(app=app_client), base_url="http://test"
        ) as client:
            resp = await client.post(self.BASE, json={})

        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_celery_failure_returns_503(self, app_client, mock_upload_record):
        db_session = _make_db_session(mock_upload_record)

        with patch("worker.api.main.get_session", return_value=db_session), \
             patch("worker.api.main.celery_app") as mock_celery, \
             patch("worker.api.main.redis_client") as mock_redis:
            mock_redis.hset = AsyncMock()
            mock_redis.expire = AsyncMock()
            mock_celery.send_task = MagicMock(side_effect=ConnectionError("broker down"))

            async with AsyncClient(
                transport=ASGITransport(app=app_client), base_url="http://test"
            ) as client:
                resp = await client.post(self.BASE, json=self.VALID_BODY)

        assert resp.status_code == 503
