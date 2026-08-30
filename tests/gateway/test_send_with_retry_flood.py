"""Tests for _send_with_retry flood-control backoff.

Regression: Telegram flood control (RetryAfter) failures were retried with a
short exponential backoff (~2s/4s) that ignored the server-mandated wait
("Retry in 27 seconds"). Both retries hit 429 again and the response was
lost with a delivery-failure notice. The retry loop must honor retry_after
(from the SendResult field, or parsed from the error string).
"""
from collections import deque
from unittest.mock import AsyncMock, patch

import pytest

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, SendResult


class FakeAdapter(BasePlatformAdapter):
    """Minimal concrete adapter exercising only _send_with_retry."""

    def __init__(self, results):
        self.platform = Platform.TELEGRAM
        self.results = deque(results)

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        pass

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        if self.results:
            return self.results.popleft()
        return SendResult(success=True)

    async def get_chat_info(self, chat_id):
        return {"name": "fake", "type": "dm"}


def make_result(**kwargs):
    base = {
        "success": False,
        "message_id": None,
        "error": "Flood control exceeded. Retry in 25 seconds",
        "raw_response": None,
        "retryable": True,
        "retry_after": None,
        "continuation_message_ids": (),
    }
    base.update(kwargs)
    return SendResult(**base)


class TestSendWithRetryFloodBackoff:
    @pytest.mark.asyncio
    async def test_retry_after_field_is_honored(self):
        adapter = FakeAdapter(
            [
                make_result(retry_after=25.0),
                SendResult(success=True),
            ]
        )
        sleeps = []
        with patch("asyncio.sleep", new=AsyncMock(side_effect=sleeps.append)), \
                patch("gateway.platforms.base.random.uniform", return_value=0.0):
            result = await adapter._send_with_retry("1", "hello")

        assert result.success is True
        assert len(sleeps) == 1
        assert sleeps[0] >= 25.0, f"retry slept {sleeps[0]}s, ignoring retry_after=25"

    @pytest.mark.asyncio
    async def test_retry_after_parsed_from_error_string(self):
        # Adapter did not pass retry_after; only the PTB error string carries it.
        adapter = FakeAdapter(
            [
                make_result(retry_after=None, error="Flood control exceeded. Retry in 17 seconds"),
                SendResult(success=True),
            ]
        )
        sleeps = []
        with patch("asyncio.sleep", new=AsyncMock(side_effect=sleeps.append)), \
                patch("gateway.platforms.base.random.uniform", return_value=0.0):
            result = await adapter._send_with_retry("1", "hello")

        assert result.success is True
        assert len(sleeps) == 1
        assert sleeps[0] >= 17.0, f"retry slept {sleeps[0]}s, ignoring parsed retry_after=17"

    @pytest.mark.asyncio
    async def test_plain_transient_keeps_short_exponential_backoff(self):
        adapter = FakeAdapter(
            [
                make_result(retry_after=None, error="Connection reset by peer"),
                SendResult(success=True),
            ]
        )
        sleeps = []
        with patch("asyncio.sleep", new=AsyncMock(side_effect=sleeps.append)), \
                patch("gateway.platforms.base.random.uniform", return_value=0.0):
            result = await adapter._send_with_retry("1", "hello")

        assert result.success is True
        assert len(sleeps) == 1
        assert sleeps[0] < 10.0, "plain transient error must keep short backoff"

    @pytest.mark.asyncio
    async def test_all_retries_fail_emits_notice(self):
        # Initial send + max_retries=2 retries = 3 failure results.
        adapter = FakeAdapter(
            [
                make_result(retry_after=2.0),
                make_result(retry_after=2.0),
                make_result(retry_after=2.0),
            ]
        )
        with patch("asyncio.sleep", new=AsyncMock()), \
                patch("gateway.platforms.base.random.uniform", return_value=0.0):
            result = await adapter._send_with_retry("1", "hello")

        assert result.success is False
