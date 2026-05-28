"""Tests for auth.py — login success, failure, and HTTP error paths."""
from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from spark_fuse.auth import AuthManager
from spark_fuse.errors import AuthError, SparkHttpError
from tests.conftest import HOST, LOGIN_FAIL, LOGIN_OK, mock_response


def _auth() -> AuthManager:
    return AuthManager(host=HOST, email="user@test.com", password="pass")


def test_login_success_stores_token():
    http = MagicMock(spec=httpx.Client)
    http.post.return_value = mock_response(200, LOGIN_OK)
    auth = _auth()
    resp = auth.login(http)
    assert resp.success is True
    assert auth.token == LOGIN_OK["token"]
    assert resp.password_expires_in_days == 45


def test_login_failure_raises_auth_error():
    http = MagicMock(spec=httpx.Client)
    http.post.return_value = mock_response(200, LOGIN_FAIL)
    auth = _auth()
    with pytest.raises(AuthError) as exc_info:
        auth.login(http)
    assert "Invalid email" in str(exc_info.value)
    assert auth.token is None


def test_login_non_200_raises_http_error():
    http = MagicMock(spec=httpx.Client)
    http.post.return_value = mock_response(500, text="Internal Server Error")
    auth = _auth()
    with pytest.raises(SparkHttpError) as exc_info:
        auth.login(http)
    assert exc_info.value.status_code == 500


def test_invalidate_clears_token():
    http = MagicMock(spec=httpx.Client)
    http.post.return_value = mock_response(200, LOGIN_OK)
    auth = _auth()
    auth.login(http)
    assert auth.token is not None
    auth.invalidate()
    assert auth.token is None


def test_login_posts_credentials():
    http = MagicMock(spec=httpx.Client)
    http.post.return_value = mock_response(200, LOGIN_OK)
    auth = _auth()
    auth.login(http)
    http.post.assert_called_once_with(
        f"{HOST}/auth/login",
        json={"email": "user@test.com", "password": "pass"},
    )
