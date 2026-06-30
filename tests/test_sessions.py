"""Tests for session lifecycle: prepare / get / release / wait_until_ready / session CM.

All HTTP is mocked — no live network calls. Matches the conftest.py pattern:
MagicMock(spec=httpx.Client) injected via http_client; mock_response() for
response fixtures; monkeypatch for time.sleep.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from spark_fuse.errors import (
    NoWarmPoolCapacityError,
    SessionConflictError,
    SessionFailedError,
    SessionNotFoundError,
    SparkHttpError,
)
from tests.conftest import LOGIN_OK, PREPARE_RESPONSE, make_client, mock_response

HANDLE = PREPARE_RESPONSE["instanceHandle"]

SESSION_READY = {
    **PREPARE_RESPONSE,
    "status": "ready",
    "readyAt": "2026-01-01T00:01:00.000Z",
}
SESSION_FAILED = {
    **PREPARE_RESPONSE,
    "status": "failed",
    "failedAt": "2026-01-01T00:02:00.000Z",
    "errorCode": "prepare_timeout",
    "errorMessage": "Instance did not start in time",
}
SESSION_RELEASED = {
    **PREPARE_RESPONSE,
    "status": "released",
    "releasedAt": "2026-01-01T00:05:00.000Z",
}


def _authed(http: MagicMock):
    http.post.return_value = mock_response(200, LOGIN_OK)
    c = make_client(http)
    c.login()
    return c


# ── prepare_instance ─────────────────────────────────────────────────────────

def test_prepare_503_raises_no_warm_pool_capacity():
    http = MagicMock(spec=httpx.Client)
    c = _authed(http)
    http.request.return_value = mock_response(503)
    with pytest.raises(NoWarmPoolCapacityError):
        c.prepare_instance(instance_type="g7e.2xlarge", hold_seconds=600)


def test_prepare_400_raises_descriptive_spark_http_error():
    http = MagicMock(spec=httpx.Client)
    c = _authed(http)
    body = {"error_code": "invalid_hold_seconds",
            "error_message": "hold_seconds must be between 60 and 3600"}
    http.request.return_value = mock_response(400, body)
    with pytest.raises(SparkHttpError) as exc_info:
        c.prepare_instance(instance_type="g7e.2xlarge", hold_seconds=0)
    assert "invalid_hold_seconds" in str(exc_info.value)
    assert exc_info.value.status_code == 400


# ── get_instance ─────────────────────────────────────────────────────────────

def test_get_instance_404_raises_session_not_found():
    http = MagicMock(spec=httpx.Client)
    c = _authed(http)
    http.request.return_value = mock_response(404, None, text="not found")
    with pytest.raises(SessionNotFoundError):
        c.get_instance("nonexistent-handle")


# ── release_instance ─────────────────────────────────────────────────────────

def test_release_409_raises_session_conflict():
    http = MagicMock(spec=httpx.Client)
    c = _authed(http)
    http.request.return_value = mock_response(409, None, text="conflict")
    with pytest.raises(SessionConflictError):
        c.release_instance(HANDLE)


# ── wait_until_ready ─────────────────────────────────────────────────────────

def test_wait_preparing_then_ready(monkeypatch):
    monkeypatch.setattr("spark_fuse.client.time.sleep", lambda _: None)
    http = MagicMock(spec=httpx.Client)
    c = _authed(http)
    # first poll: preparing; second: ready
    http.request.side_effect = [
        mock_response(200, PREPARE_RESPONSE),
        mock_response(200, SESSION_READY),
    ]
    result = c.wait_until_ready(HANDLE)
    assert result.is_ready
    assert http.request.call_count == 2


def test_wait_failed_raises_session_failed_error(monkeypatch):
    monkeypatch.setattr("spark_fuse.client.time.sleep", lambda _: None)
    http = MagicMock(spec=httpx.Client)
    c = _authed(http)
    http.request.return_value = mock_response(200, SESSION_FAILED)
    with pytest.raises(SessionFailedError) as exc_info:
        c.wait_until_ready(HANDLE)
    assert exc_info.value.error_code == "prepare_timeout"
    assert "prepare_timeout" in str(exc_info.value)


def test_wait_timeout_raises(monkeypatch):
    monkeypatch.setattr("spark_fuse.client.time.sleep", lambda _: None)
    http = MagicMock(spec=httpx.Client)
    c = _authed(http)
    http.request.return_value = mock_response(200, PREPARE_RESPONSE)  # always preparing
    with pytest.raises(TimeoutError):
        # poll_interval == timeout: first poll at waited=0 (ok), sleep, waited=3 ≥ 3 → raise
        c.wait_until_ready(HANDLE, timeout=3.0, poll_interval=3.0)


# ── session() context manager ─────────────────────────────────────────────────

def test_session_cm_releases_on_normal_exit(monkeypatch):
    monkeypatch.setattr("spark_fuse.client.time.sleep", lambda _: None)
    http = MagicMock(spec=httpx.Client)
    c = _authed(http)
    http.request.side_effect = [
        mock_response(200, PREPARE_RESPONSE),  # prepare_instance
        mock_response(200, SESSION_READY),      # wait_until_ready
        mock_response(200, SESSION_RELEASED),   # release_instance
    ]
    with c.session(instance_type="g7e.2xlarge", hold_seconds=600) as sess:
        assert sess.is_ready
    assert http.request.call_count == 3
    release_call = http.request.call_args_list[-1]
    assert "release" in release_call[0][1]


def test_session_cm_releases_when_block_raises(monkeypatch):
    monkeypatch.setattr("spark_fuse.client.time.sleep", lambda _: None)
    http = MagicMock(spec=httpx.Client)
    c = _authed(http)
    http.request.side_effect = [
        mock_response(200, PREPARE_RESPONSE),
        mock_response(200, SESSION_READY),
        mock_response(200, SESSION_RELEASED),
    ]
    with pytest.raises(RuntimeError, match="boom"):
        with c.session(instance_type="g7e.2xlarge", hold_seconds=600):
            raise RuntimeError("boom")
    assert http.request.call_count == 3
    release_call = http.request.call_args_list[-1]
    assert "release" in release_call[0][1]


def test_session_cm_releases_when_wait_until_ready_raises(monkeypatch):
    """The critical leak path: prepare succeeds but instance goes terminal-failed
    during the poll. Release must still fire even though we never yielded."""
    monkeypatch.setattr("spark_fuse.client.time.sleep", lambda _: None)
    http = MagicMock(spec=httpx.Client)
    c = _authed(http)
    http.request.side_effect = [
        mock_response(200, PREPARE_RESPONSE),  # prepare_instance succeeds
        mock_response(200, SESSION_FAILED),    # wait_until_ready sees terminal-failed
        mock_response(200, SESSION_RELEASED),  # release_instance must still fire
    ]
    with pytest.raises(SessionFailedError):
        with c.session(instance_type="g7e.2xlarge", hold_seconds=600):
            pass  # never reached
    assert http.request.call_count == 3
    release_call = http.request.call_args_list[-1]
    assert "release" in release_call[0][1]
