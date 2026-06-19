"""Tests for client.py — API operations and retry logic (all HTTP mocked)."""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import httpx
import pytest

from spark_fuse.client import SparkFuseClient
from spark_fuse.errors import (
    RateLimitError,
    ServiceUnavailableError,
    TokenExpiredError,
)
from spark_fuse.models import Job, JobStatus
from tests.conftest import (
    ESTIMATE_RESPONSE,
    HOST,
    JOB_RESPONSE,
    LOGIN_OK,
    SKUS_RESPONSE,
    SUBMIT_RESPONSE,
    make_client,
    mock_response,
)


def _authed_client(http: MagicMock) -> SparkFuseClient:
    """Return a client that's already logged in."""
    http.post.return_value = mock_response(200, LOGIN_OK)
    c = make_client(http)
    c.login()
    return c


# ------------------------------------------------------------------
# list_skus
# ------------------------------------------------------------------

def test_list_skus_returns_list():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    http.request.return_value = mock_response(200, SKUS_RESPONSE)
    skus = c.list_skus()
    assert skus == ["g4dn.xlarge", "g5.xlarge", "g7e.2xlarge"]


# ------------------------------------------------------------------
# estimate
# ------------------------------------------------------------------

def test_estimate_parses_rate_and_total():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    http.request.return_value = mock_response(200, ESTIMATE_RESPONSE)
    result = c.estimate(instance_type="g4dn.xlarge", estimated_runtime_seconds=3600)
    assert result.instance_type == "g4dn.xlarge"
    assert result.rate.billed_per_hour_usd == "0.36"
    assert result.estimate is not None
    assert result.estimate.total_usd == "0.36"


def test_estimate_rate_only_when_no_runtime():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    rate_only = {**ESTIMATE_RESPONSE, "estimate": None}
    # None estimate block
    import json as _json
    rate_only_resp = {
        "instanceType": "g4dn.xlarge",
        "mode": "instant",
        "rate": {"billedPerSecondCents": "0.01", "billedPerHourUsd": "0.36"},
        "notes": [],
    }
    http.request.return_value = mock_response(200, rate_only_resp)
    result = c.estimate(instance_type="g4dn.xlarge")
    assert result.estimate is None


# ------------------------------------------------------------------
# submit
# ------------------------------------------------------------------

def test_submit_minimal():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    http.request.return_value = mock_response(200, SUBMIT_RESPONSE)
    resp = c.submit(image="alpine:3", command=["echo", "hi"], instance_type="g4dn.xlarge")
    assert resp.job_id == SUBMIT_RESPONSE["jobId"]
    assert resp.status == "queued"
    assert resp.input is None


def test_submit_sends_correct_body():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    http.request.return_value = mock_response(200, SUBMIT_RESPONSE)
    c.submit(
        image="alpine:3",
        command=["echo", "hello"],
        instance_type="g4dn.xlarge",
        tags=["ci", "test"],
        mode="instant",
    )
    _, kwargs = http.request.call_args
    body = kwargs["json"]
    assert body["image"] == "alpine:3"
    assert body["command"] == ["echo", "hello"]
    assert body["instanceType"] == "g4dn.xlarge"
    assert body["tags"] == ["ci", "test"]
    assert body["mode"] == "instant"
    # Optional fields omitted when None
    assert "env" not in body
    assert "inputPushMode" not in body


def test_submit_push_mode_included():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    push_response = {
        **SUBMIT_RESPONSE,
        "input": {
            "shareSyncPath": "/Spark Fuse Job Inputs/abc/",
            "shareSyncSpaceName": None,
            "shareSyncBaseUrl": None,
            "uploadUrl": "https://files.example.com/upload/spark-input.tar.gz",
            "uploadMethod": "PUT",
        },
    }
    http.request.return_value = mock_response(200, push_response)
    resp = c.submit(
        image="alpine:3",
        command=["ls"],
        instance_type="g4dn.xlarge",
        input_push_mode="auto-prepare",
    )
    assert resp.input is not None
    assert resp.input.upload_url == "https://files.example.com/upload/spark-input.tar.gz"


def test_submit_assets_and_affinity_in_body():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    http.request.return_value = mock_response(200, SUBMIT_RESPONSE)
    c.submit(
        image="ghcr.io/org/comfyui@sha256:abc",
        command=["python3.13", "/runner/spark_fuse_run.py"],
        instance_type="g7e.2xlarge",
        env={"MODEL_BASE_DIR": "/assets"},
        input_share_sync_path="/jobs/klein/",
        assets_share_sync_path="/comfy-flux2-klein/models",
        image_affinity="required",
    )
    _, kwargs = http.request.call_args
    body = kwargs["json"]
    assert body["assetsShareSyncPath"] == "/comfy-flux2-klein/models"
    assert body["imageAffinity"] == "required"
    assert body["env"] == {"MODEL_BASE_DIR": "/assets"}
    assert body["inputShareSyncPath"] == "/jobs/klein/"


# ------------------------------------------------------------------
# get_job
# ------------------------------------------------------------------

def test_get_job_parses_full_row():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    http.request.return_value = mock_response(200, JOB_RESPONSE)
    job = c.get_job("abc12345-0000-0000-0000-000000000001")
    assert job.id == JOB_RESPONSE["id"]
    assert job.status == "succeeded"
    assert job.is_terminal is True
    assert job.exit_code == 0


def test_get_job_non_terminal():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    running = {**JOB_RESPONSE, "status": "running", "terminal_at": None, "exit_code": None}
    http.request.return_value = mock_response(200, running)
    job = c.get_job("abc12345-0000-0000-0000-000000000001")
    assert job.is_terminal is False


# ------------------------------------------------------------------
# list_jobs
# ------------------------------------------------------------------

def test_list_jobs_returns_all():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    http.request.return_value = mock_response(200, {"jobs": [JOB_RESPONSE, JOB_RESPONSE]})
    jobs = c.list_jobs()
    assert len(jobs) == 2


def test_list_jobs_tag_filter_params():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    http.request.return_value = mock_response(200, {"jobs": []})
    c.list_jobs(tags=["ci", "training"])
    _, kwargs = http.request.call_args
    params = kwargs["params"]
    assert ("tag", "ci") in params
    assert ("tag", "training") in params


def test_list_jobs_tags_any_filter():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    http.request.return_value = mock_response(200, {"jobs": []})
    c.list_jobs(tags_any="ci,training")
    _, kwargs = http.request.call_args
    assert ("tagsAny", "ci,training") in kwargs["params"]


# ------------------------------------------------------------------
# cancel
# ------------------------------------------------------------------

def test_cancel_returns_job():
    http = MagicMock(spec=httpx.Client)
    c = _authed_client(http)
    cancelled = {**JOB_RESPONSE, "status": "cancelled"}
    http.request.return_value = mock_response(200, cancelled)
    job = c.cancel("abc12345-0000-0000-0000-000000000001")
    assert job.status == "cancelled"
    method, url = http.request.call_args[0]
    assert method == "POST"
    assert "cancel" in url


# ------------------------------------------------------------------
# 401 re-login retry
# ------------------------------------------------------------------

def test_request_re_logins_on_401_and_succeeds():
    http = MagicMock(spec=httpx.Client)
    # First call: 401; second call after re-login: 200
    http.request.side_effect = [
        mock_response(401),
        mock_response(200, SKUS_RESPONSE),
    ]
    # Re-login returns a fresh token
    http.post.side_effect = [
        mock_response(200, LOGIN_OK),  # initial login
        mock_response(200, LOGIN_OK),  # re-login after 401
    ]
    c = make_client(http)
    c.login()
    skus = c.list_skus()
    assert skus == ["g4dn.xlarge", "g5.xlarge", "g7e.2xlarge"]
    assert http.post.call_count == 2  # initial + re-login


def test_request_raises_after_double_401():
    http = MagicMock(spec=httpx.Client)
    http.request.return_value = mock_response(401)
    http.post.return_value = mock_response(200, LOGIN_OK)
    c = make_client(http)
    c.login()
    with pytest.raises(TokenExpiredError):
        c.list_skus()


# ------------------------------------------------------------------
# 429 rate limit
# ------------------------------------------------------------------

def test_request_raises_rate_limit_on_429():
    http = MagicMock(spec=httpx.Client)
    rate_resp = mock_response(429)
    rate_resp.headers = {"retry-after": "30"}
    http.request.return_value = rate_resp
    http.post.return_value = mock_response(200, LOGIN_OK)
    c = make_client(http)
    c.login()
    with pytest.raises(RateLimitError) as exc_info:
        c.list_skus()
    assert exc_info.value.retry_after == 30.0


# ------------------------------------------------------------------
# 503 retry backoff
# ------------------------------------------------------------------

def test_request_retries_503_and_eventually_raises(monkeypatch):
    monkeypatch.setattr("spark_fuse.client.time.sleep", lambda _: None)
    http = MagicMock(spec=httpx.Client)
    http.request.return_value = mock_response(503)
    http.post.return_value = mock_response(200, LOGIN_OK)
    c = make_client(http)
    c.login()
    with pytest.raises(ServiceUnavailableError):
        c.list_skus()
    # Should have retried _MAX_503_RETRIES + 1 times total
    assert http.request.call_count == 4  # 1 initial + 3 retries


def test_request_succeeds_after_one_503(monkeypatch):
    monkeypatch.setattr("spark_fuse.client.time.sleep", lambda _: None)
    http = MagicMock(spec=httpx.Client)
    http.request.side_effect = [
        mock_response(503),
        mock_response(200, SKUS_RESPONSE),
    ]
    http.post.return_value = mock_response(200, LOGIN_OK)
    c = make_client(http)
    c.login()
    skus = c.list_skus()
    assert skus == ["g4dn.xlarge", "g5.xlarge", "g7e.2xlarge"]
