"""Tests for logs.py — SSE event parsing (httpx_sse mocked)."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from spark_fuse.logs import stream_logs
from spark_fuse.models import LogEvent, QueueStatusEvent, TruncatedEvent

URL = "https://api.example.com/api/compute/jobs/abc/logs/stream"
TOKEN = "fake-token"


def _make_sse(event: str, data: dict, event_id: str | None = None) -> MagicMock:
    sse = MagicMock()
    sse.event = event
    sse.data = json.dumps(data)
    sse.id = event_id
    return sse


def _patch_connect_sse(events: list[MagicMock]):
    mock_source = MagicMock()
    mock_source.iter_sse.return_value = iter(events)
    mock_source.__enter__ = MagicMock(return_value=mock_source)
    mock_source.__exit__ = MagicMock(return_value=False)
    return patch("spark_fuse.logs.httpx_sse.connect_sse", return_value=mock_source)


def test_stream_logs_yields_log_event():
    log_data = {"ts": "2026-01-01T00:00:01Z", "stream": "stdout", "line": "hello world", "phase": "container"}
    sse_events = [_make_sse("log", log_data, event_id="123-stdout")]

    http = MagicMock(spec=httpx.Client)
    with _patch_connect_sse(sse_events):
        events = list(stream_logs(URL, TOKEN, client=http))

    assert len(events) == 1
    evt = events[0]
    assert isinstance(evt, LogEvent)
    assert evt.line == "hello world"
    assert evt.stream == "stdout"
    assert evt.event_id == "123-stdout"


def test_stream_logs_yields_queue_status():
    qs_data = {"status": "queued", "queuePosition": 2, "estimatedStartSeconds": 120, "done": False}
    sse_events = [_make_sse("queue.status", qs_data)]

    http = MagicMock(spec=httpx.Client)
    with _patch_connect_sse(sse_events):
        events = list(stream_logs(URL, TOKEN, client=http))

    assert len(events) == 1
    evt = events[0]
    assert isinstance(evt, QueueStatusEvent)
    assert evt.queue_position == 2
    assert evt.done is False


def test_stream_logs_yields_truncated():
    trunc_data = {"reason": "buffer_full", "dropped": 50}
    sse_events = [_make_sse("truncated", trunc_data)]

    http = MagicMock(spec=httpx.Client)
    with _patch_connect_sse(sse_events):
        events = list(stream_logs(URL, TOKEN, client=http))

    assert len(events) == 1
    assert isinstance(events[0], TruncatedEvent)
    assert events[0].data["dropped"] == 50


def test_stream_logs_skips_unknown_event_types():
    sse_events = [
        _make_sse("unknown.type", {"foo": "bar"}),
        _make_sse("log", {"ts": "t", "stream": "stdout", "line": "real line", "phase": "container"}),
    ]
    http = MagicMock(spec=httpx.Client)
    with _patch_connect_sse(sse_events):
        events = list(stream_logs(URL, TOKEN, client=http))

    assert len(events) == 1
    assert isinstance(events[0], LogEvent)


def test_stream_logs_skips_empty_events():
    empty_sse = MagicMock()
    empty_sse.event = ""
    empty_sse.data = ""
    log_data = {"ts": "t", "stream": "stderr", "line": "err", "phase": "container"}
    sse_events = [empty_sse, _make_sse("log", log_data)]

    http = MagicMock(spec=httpx.Client)
    with _patch_connect_sse(sse_events):
        events = list(stream_logs(URL, TOKEN, client=http))

    assert len(events) == 1


def test_stream_logs_passes_bearer_token():
    http = MagicMock(spec=httpx.Client)
    with _patch_connect_sse([]) as mock_connect:
        list(stream_logs(URL, TOKEN, client=http))
    _, kwargs = mock_connect.call_args
    assert kwargs["headers"]["Authorization"] == f"Bearer {TOKEN}"


def test_stream_logs_mixed_events():
    events_raw = [
        _make_sse("queue.status", {"status": "queued", "queuePosition": 1, "estimatedStartSeconds": 60, "done": False}),
        _make_sse("queue.status", {"status": "running", "queuePosition": None, "estimatedStartSeconds": None, "done": True}),
        _make_sse("log", {"ts": "t1", "stream": "stdout", "line": "line 1", "phase": "container"}),
        _make_sse("log", {"ts": "t2", "stream": "stderr", "line": "line 2", "phase": "container"}),
    ]
    http = MagicMock(spec=httpx.Client)
    with _patch_connect_sse(events_raw):
        events = list(stream_logs(URL, TOKEN, client=http))

    assert len(events) == 4
    assert isinstance(events[0], QueueStatusEvent)
    assert isinstance(events[1], QueueStatusEvent)
    assert events[1].done is True
    assert isinstance(events[2], LogEvent)
    assert isinstance(events[3], LogEvent)
    assert events[3].stream == "stderr"
