"""Authentication: POST /auth/login and bearer-token lifecycle.

The API always returns HTTP 200 from /auth/login — success or failure is
determined by the 'success' boolean in the body, not the status code (§1.1).
There is no refresh endpoint; re-call login() with the same credentials
whenever a 401 is received from a compute endpoint (§1.3).
"""
from __future__ import annotations

import logging

import httpx

from .errors import AuthError, SparkHttpError
from .models import LoginResponse

log = logging.getLogger(__name__)


class AuthManager:
    """Holds credentials and caches the current bearer token.

    Token is treated as transient — never persisted to disk.
    """

    def __init__(self, host: str, email: str, password: str) -> None:
        self._host = host.rstrip("/")
        self._email = email
        self._password = password
        self._token: str | None = None

    @property
    def token(self) -> str | None:
        return self._token

    def login(self, client: httpx.Client) -> LoginResponse:
        """POST /auth/login with stored credentials.

        Raises AuthError if success=false.
        Never logs the full token — only the first 20 chars as a prefix.
        """
        url = f"{self._host}/auth/login"
        resp = client.post(url, json={"email": self._email, "password": self._password})
        if not resp.is_success:
            raise SparkHttpError(resp.status_code, resp.text)
        data = resp.json()
        login_resp = LoginResponse.from_dict(data)
        # Branch on success BEFORE reading token — failed logins return HTTP 200
        # with success=false and token=null (§1.1)
        if not login_resp.success:
            raise AuthError(login_resp.resp)
        self._token = login_resp.token
        log.debug("Authenticated; token prefix=%s...", (self._token or "")[:20])
        return login_resp

    def invalidate(self) -> None:
        """Clear cached token so the next _ensure_token() call re-logs in."""
        self._token = None
