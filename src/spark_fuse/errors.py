"""Typed exceptions for Spark Fuse API errors."""
from __future__ import annotations


class SparkFuseError(Exception):
    """Base exception for all Spark Fuse errors."""


class AuthError(SparkFuseError):
    """Login failed (success=false from server)."""

    def __init__(self, resp: str) -> None:
        self.resp = resp
        super().__init__(f"Authentication failed: {resp}")


class TokenExpiredError(SparkFuseError):
    """Token expired/invalid (401) and could not be recovered by re-login."""


class ForbiddenError(SparkFuseError):
    """Token valid but belongs to a different org (403)."""


class SparkHttpError(SparkFuseError):
    """Unexpected HTTP error from the API."""

    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text
        super().__init__(f"HTTP {status_code}: {text[:200]}")


class RateLimitError(SparkFuseError):
    """Rate-limited (429). retry_after carries the Retry-After value in seconds if present."""

    def __init__(self, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        msg = "Rate limited (HTTP 429)"
        if retry_after is not None:
            msg += f"; retry after {retry_after:.0f}s"
        super().__init__(msg)


class ServiceUnavailableError(SparkFuseError):
    """Service unavailable (503) after configured retries."""


class ShareSyncError(SparkFuseError):
    """WebDAV / ShareSync operation failed."""


class SessionError(SparkFuseError):
    """Base class for session (prepared-instance) errors."""


class NoWarmPoolCapacityError(SessionError):
    """No warm-pool capacity available (HTTP 503 on prepare)."""


class SessionFailedError(SessionError):
    """Session entered a terminal 'failed' state."""

    def __init__(self, error_code: str | None, error_message: str | None) -> None:
        self.error_code = error_code
        self.error_message = error_message
        detail = " ".join(filter(None, [error_code, error_message])) or "session failed"
        super().__init__(f"Session failed: {detail}")


class SessionNotFoundError(SessionError):
    """Session handle not found (HTTP 404)."""


class SessionConflictError(SessionError):
    """Session state conflict (HTTP 409); the session may already be released."""
