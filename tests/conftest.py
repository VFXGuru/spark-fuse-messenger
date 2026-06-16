"""Shared test helpers and fixtures.

All HTTP is mocked — no live network calls.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import httpx
import pytest

from spark_fuse.client import SparkFuseClient

HOST = "https://api.example.sparkcloud.studio"

LOGIN_OK = {
    "success": True,
    "token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.fake_payload.fake_sig",
    "resp": "Login Successful",
    "password_expired": False,
    "password_expires_in_days": 45,
    "requires_password_change": False,
}

LOGIN_FAIL = {
    "success": False,
    "token": None,
    "resp": "Invalid email or password. Please check your credentials and try again.",
    "password_expired": False,
    "password_expires_in_days": None,
    "requires_password_change": False,
}

SUBMIT_RESPONSE = {
    "jobId": "abc12345-0000-0000-0000-000000000001",
    "status": "queued",
    "imageDigest": "sha256:aabbcc",
    "outputShareSyncPath": "/Spark Fuse Jobs/abc12345-0000-0000-0000-000000000001/",
    "createdAt": "2026-01-01T00:00:00.000Z",
    "output": {
        "shareSyncPath": "/Spark Fuse Jobs/abc12345-0000-0000-0000-000000000001/",
        "shareSyncSpaceName": None,
        "shareSyncBaseUrl": "https://org.files.sparkcloud.studio/dav/spaces/s1/Spark%20Fuse%20Jobs/abc12345-0000-0000-0000-000000000001/",
    },
    "input": None,
    "queuePosition": 0,
    "estimatedStartSeconds": 5,
    "shmSize": "2g",
    "notifyOnFailure": None,
    "maxWallClockSeconds": None,
    "containerInactivitySeconds": None,
}

JOB_RESPONSE = {
    "id": "abc12345-0000-0000-0000-000000000001",
    "image": "alpine:3",
    "command": ["echo", "hello"],
    "instance_type_name": "g4dn.xlarge",
    "mode": "instant",
    "status": "succeeded",
    "error_code": None,
    "error_message": None,
    "exit_code": 0,
    "created_at": "2026-01-01T00:00:00.000Z",
    "terminal_at": "2026-01-01T00:01:00.000Z",
    "output": {
        "shareSyncPath": "/Spark Fuse Jobs/abc12345-0000-0000-0000-000000000001/",
        "shareSyncSpaceName": None,
        "shareSyncBaseUrl": "https://org.files.sparkcloud.studio/dav/spaces/s1/Spark%20Fuse%20Jobs/abc12345-0000-0000-0000-000000000001/",
    },
    "input": None,
}

SKUS_RESPONSE = {"skus": ["g4dn.xlarge", "g5.xlarge", "g7e.2xlarge"]}

ESTIMATE_RESPONSE = {
    "instanceType": "g4dn.xlarge",
    "mode": "instant",
    "rate": {"billedPerSecondCents": "0.01", "billedPerHourUsd": "0.36"},
    "estimate": {"billableSeconds": 3600, "totalCents": "36.00", "totalUsd": "0.36"},
    "notes": ["Quotes are estimates."],
}


def mock_response(status_code: int, data: dict | list | None = None, text: str = "") -> MagicMock:
    """Build a mock httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.is_success = 200 <= status_code < 300
    if data is not None:
        resp.json.return_value = data
        resp.text = json.dumps(data)
    else:
        resp.text = text
        resp.json.side_effect = ValueError("not json")
    resp.headers = {}
    return resp


def make_client(http: MagicMock, stream: MagicMock | None = None) -> SparkFuseClient:
    return SparkFuseClient(
        host=HOST,
        email="test@test.com",
        password="secret",
        http_client=http,
        stream_client=stream or MagicMock(spec=httpx.Client),
    )
