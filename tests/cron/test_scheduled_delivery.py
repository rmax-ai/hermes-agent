"""Tests for scheduled-delivery receipts and retry classification."""

import asyncio
import copy
from concurrent.futures import Future
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from gateway.config import Platform
from gateway.platforms.base import SendResult
import cron.scheduler as scheduler


def _delivery_setup(job):
    stored_jobs = [copy.deepcopy(job)]
    calls = []

    class StubTelegramAdapter:
        async def send_scheduled_output(
            self,
            text,
            *,
            destination,
            run_id,
            metadata=None,
            job_name="cron job",
        ):
            calls.append(
                {
                    "text": text,
                    "destination": destination,
                    "run_id": run_id,
                    "metadata": metadata,
                    "job_name": job_name,
                }
            )
            return SendResult(success=True, message_id="telegram-42")

    adapter = StubTelegramAdapter()
    config = SimpleNamespace(
        platforms={Platform.TELEGRAM: SimpleNamespace(enabled=True)}
    )
    loop = MagicMock()
    loop.is_running.return_value = True

    def run_coro(coro, _loop):
        future = Future()
        try:
            future.set_result(asyncio.run(coro))
        except BaseException as exc:
            future.set_exception(exc)
        return future

    patches = [
        patch("gateway.config.load_gateway_config", return_value=config),
        patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}),
        patch("cron.scheduler.load_jobs", side_effect=lambda: stored_jobs),
        patch("cron.scheduler.save_jobs", side_effect=lambda jobs: None),
        patch("cron.scheduler._jobs_lock", return_value=nullcontext()),
        patch("asyncio.run_coroutine_threadsafe", side_effect=run_coro),
    ]
    return adapter, loop, calls, stored_jobs, patches


def test_successful_receipt_suppresses_same_run_and_payload():
    job = {
        "id": "digest-job",
        "name": "Digest",
        "next_run_at": "2026-10-06T09:00:00+00:00",
        "deliver": "origin",
        "origin": {"platform": "telegram", "chat_id": "123", "thread_id": "17"},
    }
    adapter, loop, calls, stored_jobs, patches = _delivery_setup(job)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        first = scheduler._deliver_result(
            job,
            "# " + "x" * 4500,
            adapters={Platform.TELEGRAM: adapter},
            loop=loop,
            run_id="run-1",
        )
        second = scheduler._deliver_result(
            job,
            "# " + "x" * 4500,
            adapters={Platform.TELEGRAM: adapter},
            loop=loop,
            run_id="run-1",
        )

    assert first is None
    assert second is None
    assert len(calls) == 1
    assert calls[0]["run_id"] == "run-1"
    assert calls[0]["metadata"] == {"thread_id": "17"}
    receipt = stored_jobs[0]["last_delivery_receipt"]
    assert set(receipt) == {
        "run_id",
        "payload_sha256",
        "delivery_kind",
        "platform_message_id",
        "delivered_at",
    }
    assert receipt["delivery_kind"] == "document"
    assert receipt["platform_message_id"] == "telegram-42"


def test_timeout_is_visible_and_same_attempt_is_not_resubmitted():
    job = {
        "id": "timeout-job",
        "name": "Timeout digest",
        "next_run_at": "2026-10-06T09:00:00+00:00",
        "deliver": "origin",
        "origin": {"platform": "telegram", "chat_id": "123"},
    }
    stored_jobs = [copy.deepcopy(job)]
    calls = []

    class StubTelegramAdapter:
        def send_scheduled_output(self, text, **kwargs):
            calls.append((text, kwargs))
            async def pending():
                return SendResult(success=True, message_id="never-used")
            return pending()

    adapter = StubTelegramAdapter()
    loop = MagicMock()
    loop.is_running.return_value = True
    future = Future()
    cancelled = []
    future.cancel = lambda: cancelled.append(True) or True
    future.result = MagicMock(side_effect=TimeoutError("request timed out"))

    with patch("gateway.config.load_gateway_config", return_value=SimpleNamespace(
        platforms={Platform.TELEGRAM: SimpleNamespace(enabled=True)}
    )), patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
         patch("cron.scheduler.load_jobs", side_effect=lambda: stored_jobs), \
         patch("cron.scheduler.save_jobs"), \
         patch("cron.scheduler._jobs_lock", return_value=nullcontext()), \
         patch("asyncio.run_coroutine_threadsafe", return_value=future):
        first = scheduler._deliver_result(
            job,
            "x" * 4500,
            adapters={Platform.TELEGRAM: adapter},
            loop=loop,
            run_id="run-timeout",
        )
        second = scheduler._deliver_result(
            job,
            "x" * 4500,
            adapters={Platform.TELEGRAM: adapter},
            loop=loop,
            run_id="run-timeout",
        )

    assert "ambiguous_timeout" in first
    assert "ambiguous_timeout" in second
    assert len(calls) == 1
    assert cancelled == [True]
    assert stored_jobs[0]["last_delivery_attempt"]["status"] == "ambiguous_timeout"
