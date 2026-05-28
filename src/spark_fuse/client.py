"""Main SparkFuseClient — credentials + all API operations in one place."""
from __future__ import annotations

import logging
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import httpx

from .auth import AuthManager
from .errors import (
    ForbiddenError,
    RateLimitError,
    ServiceUnavailableError,
    SparkHttpError,
    TokenExpiredError,
)
from .logs import SSEEvent
from .logs import stream_logs as _stream_logs
from .models import (
    CreateJobResponse,
    EstimateResponse,
    Job,
    LoginResponse,
    ShareSyncEntry,
)
from .sharesync import download_file, file_url_from_entry, propfind, upload_directory

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = httpx.Timeout(30.0)
_MAX_503_RETRIES = 3
_503_BACKOFF_BASE = 1.0  # seconds; doubles each retry (1s, 2s, 4s)


class SparkFuseClient:
    """Client for the Spark Fuse GPU compute API.

    Handles authentication, 401-on-expiry re-login, 429/503 retry/backoff,
    and all documented v1 endpoints.

    Parameters
    ----------
    host:
        Base URL, e.g. ``https://api.prod.aapse1.sparkcloud.studio``.
    email / password:
        Spark Fuse credentials. Never logged; password never stored beyond
        the auth manager.
    timeout:
        httpx Timeout for normal (non-streaming) calls.
    http_client / stream_client:
        Optional pre-built httpx.Client instances — mainly useful for testing.
    """

    def __init__(
        self,
        host: str,
        email: str,
        password: str,
        *,
        timeout: httpx.Timeout = _DEFAULT_TIMEOUT,
        http_client: httpx.Client | None = None,
        stream_client: httpx.Client | None = None,
    ) -> None:
        self._host = host.rstrip("/")
        self._auth = AuthManager(host=self._host, email=email, password=password)
        self._http = http_client or httpx.Client(timeout=timeout)
        # SSE streams must not have a read timeout — they stay open for the job lifetime
        self._stream_http = stream_client or httpx.Client(timeout=httpx.Timeout(None))

    def close(self) -> None:
        self._http.close()
        self._stream_http.close()

    def __enter__(self) -> "SparkFuseClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def login(self) -> LoginResponse:
        """Authenticate and cache the bearer token. Returns the full login response."""
        return self._auth.login(self._http)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_token(self) -> str:
        if self._auth.token is None:
            self._auth.login(self._http)
        assert self._auth.token is not None
        return self._auth.token

    def _request(
        self,
        method: str,
        path: str,
        *,
        _retry_auth: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        """Authenticated request with 401 re-login, 429, and 503 handling.

        On 401: re-calls login() once and retries. If still 401, raises
        TokenExpiredError (§1.3).
        On 429: raises RateLimitError with the Retry-After value if present (§12.8).
        On 503: retries up to _MAX_503_RETRIES times with exponential backoff (§2.4).
        """
        token = self._ensure_token()
        extra_headers: dict[str, str] = kwargs.pop("headers", {})
        headers = {"Authorization": f"Bearer {token}", **extra_headers}

        for attempt in range(_MAX_503_RETRIES + 1):
            resp = self._http.request(
                method,
                f"{self._host}{path}",
                headers=headers,
                **kwargs,
            )

            if resp.status_code == 401:
                if _retry_auth:
                    log.debug("Got 401; re-authenticating and retrying once")
                    self._auth.invalidate()
                    self._auth.login(self._http)
                    headers["Authorization"] = f"Bearer {self._auth.token}"
                    resp = self._http.request(
                        method,
                        f"{self._host}{path}",
                        headers=headers,
                        **kwargs,
                    )
                    if resp.status_code == 401:
                        raise TokenExpiredError(
                            "Re-login succeeded but request still returned 401"
                        )
                else:
                    raise TokenExpiredError("Request returned 401")

            if resp.status_code == 403:
                raise ForbiddenError(f"Access denied (403): {resp.text[:200]}")

            if resp.status_code == 429:
                raise RateLimitError(_parse_retry_after(resp))

            if resp.status_code == 503:
                if attempt < _MAX_503_RETRIES:
                    wait = _503_BACKOFF_BASE * (2 ** attempt)
                    log.warning(
                        "HTTP 503 (attempt %d/%d); retrying in %.1fs",
                        attempt + 1,
                        _MAX_503_RETRIES,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                raise ServiceUnavailableError(
                    f"Service unavailable after {_MAX_503_RETRIES} retries"
                )

            return resp

        raise ServiceUnavailableError("Exhausted 503 retries")  # should be unreachable

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

    def submit(
        self,
        *,
        image: str,
        command: list[str],
        instance_type: str,
        env: dict[str, str] | None = None,
        start_script_b64: str | None = None,
        mode: str | None = None,
        max_retries_on_interrupt: int | None = None,
        output_share_sync_path: str | None = None,
        output_share_sync_space_name: str | None = None,
        input_share_sync_path: str | None = None,
        input_share_sync_space_name: str | None = None,
        input_push_mode: str | None = None,
        webhook_endpoint_id: str | None = None,
        idle_hold_seconds: int | None = None,
        shm_size: str | None = None,
        instance_handle: str | None = None,
        tags: list[str] | None = None,
        notify_on_failure: bool | None = None,
        max_wall_clock_seconds: int | None = None,
        container_inactivity_seconds: int | None = None,
    ) -> CreateJobResponse:
        """POST /api/compute/jobs — submit a compute job (§2).

        Required: image, command, instance_type.

        Input workflows (mutually exclusive — server returns 400 if both given):
          input_push_mode='auto-prepare'   — server allocates an upload URL (§3.1)
          input_share_sync_path            — mount a pre-populated ShareSync path (§3.2)

        After an auto-prepare submit, call upload_input(local_dir, response.input.upload_url)
        within 5 minutes, otherwise the job fails with input_download_failed.
        """
        body: dict[str, Any] = {
            "image": image,
            "command": command,
            "instanceType": instance_type,
        }
        _opt(body, "env", env)
        _opt(body, "startScriptB64", start_script_b64)
        _opt(body, "mode", mode)
        _opt(body, "maxRetriesOnInterrupt", max_retries_on_interrupt)
        _opt(body, "outputShareSyncPath", output_share_sync_path)
        _opt(body, "outputShareSyncSpaceName", output_share_sync_space_name)
        _opt(body, "inputShareSyncPath", input_share_sync_path)
        _opt(body, "inputShareSyncSpaceName", input_share_sync_space_name)
        _opt(body, "inputPushMode", input_push_mode)
        _opt(body, "webhookEndpointId", webhook_endpoint_id)
        _opt(body, "idleHoldSeconds", idle_hold_seconds)
        _opt(body, "shmSize", shm_size)
        _opt(body, "instanceHandle", instance_handle)
        _opt(body, "tags", tags)
        _opt(body, "notifyOnFailure", notify_on_failure)
        _opt(body, "maxWallClockSeconds", max_wall_clock_seconds)
        _opt(body, "containerInactivitySeconds", container_inactivity_seconds)

        resp = self._request("POST", "/api/compute/jobs", json=body)
        if resp.status_code != 200:
            raise SparkHttpError(resp.status_code, resp.text)
        return CreateJobResponse.from_dict(resp.json())

    def get_job(self, job_id: str) -> Job:
        """GET /api/compute/jobs/{job_id} — full job row with resolved URLs (§5)."""
        resp = self._request("GET", f"/api/compute/jobs/{job_id}")
        if resp.status_code != 200:
            raise SparkHttpError(resp.status_code, resp.text)
        return Job.from_dict(resp.json())

    def list_jobs(
        self,
        *,
        tags: list[str] | None = None,
        tags_any: str | None = None,
    ) -> list[Job]:
        """GET /api/compute/jobs — list jobs for your org (§6).

        tags:     AND semantics — job must carry every listed tag.
        tags_any: OR semantics — comma-separated; job must carry at least one.
        Passing both raises 400 on the server (error_code='invalid_tags').

        Note: output.share_sync_base_url is null in list responses.
        Call get_job() to retrieve the resolved download URL for a specific job.
        """
        params: list[tuple[str, str]] = []
        if tags:
            for t in tags:
                params.append(("tag", t))
        if tags_any:
            params.append(("tagsAny", tags_any))

        resp = self._request(
            "GET",
            "/api/compute/jobs",
            params=params if params else None,
        )
        if resp.status_code != 200:
            raise SparkHttpError(resp.status_code, resp.text)
        return [Job.from_dict(j) for j in resp.json().get("jobs", [])]

    def cancel(self, job_id: str) -> Job:
        """POST /api/compute/jobs/{job_id}/cancel — idempotent (§7).

        Cancelling a terminal job returns the job row unchanged.
        Cancel always skips any remaining idle-hold window.
        """
        resp = self._request("POST", f"/api/compute/jobs/{job_id}/cancel")
        if resp.status_code != 200:
            raise SparkHttpError(resp.status_code, resp.text)
        return Job.from_dict(resp.json())

    def stream_logs(self, job_id: str) -> Generator[SSEEvent, None, None]:
        """GET /api/compute/jobs/{job_id}/logs/stream as SSE (§4).

        Yields QueueStatusEvent, LogEvent, or TruncatedEvent.
        The generator exhausts when the server closes the stream (job terminal).

        No historical replay — connect immediately after submit.
        """
        token = self._ensure_token()
        url = f"{self._host}/api/compute/jobs/{job_id}/logs/stream"
        return _stream_logs(url, token, client=self._stream_http)

    def list_skus(self) -> list[str]:
        """GET /api/compute/skus — eligible instance type names (§8)."""
        resp = self._request("GET", "/api/compute/skus")
        if resp.status_code != 200:
            raise SparkHttpError(resp.status_code, resp.text)
        return resp.json().get("skus", [])

    def estimate(
        self,
        *,
        instance_type: str,
        mode: str | None = None,
        estimated_runtime_seconds: int | None = None,
        idle_hold_seconds: int | None = None,
    ) -> EstimateResponse:
        """POST /api/compute/jobs/estimate — cost quote without submitting (§2.8).

        Omit estimated_runtime_seconds to get rate-only output (no estimate block).
        """
        body: dict[str, Any] = {"instanceType": instance_type}
        _opt(body, "mode", mode)
        _opt(body, "estimatedRuntimeSeconds", estimated_runtime_seconds)
        _opt(body, "idleHoldSeconds", idle_hold_seconds)

        resp = self._request("POST", "/api/compute/jobs/estimate", json=body)
        if resp.status_code != 200:
            raise SparkHttpError(resp.status_code, resp.text)
        return EstimateResponse.from_dict(resp.json())

    # ------------------------------------------------------------------
    # Input upload — push workflow (§3.1)
    # ------------------------------------------------------------------

    def upload_input(self, local_dir: Path, upload_url: str) -> None:
        """Tar+gzip *local_dir* and PUT it to the one-shot *upload_url* (§3.1).

        Must be called within 5 minutes of submit.
        Uses stdlib tarfile — no external tar binary required.
        """
        token = self._ensure_token()
        upload_directory(Path(local_dir), upload_url, token, client=self._http)

    # ------------------------------------------------------------------
    # Output download — ShareSync WebDAV (§9)
    # ------------------------------------------------------------------

    def list_outputs(self, share_sync_base_url: str) -> list[ShareSyncEntry]:
        """PROPFIND *share_sync_base_url* with Depth:1 — list immediate contents (§9.1)."""
        token = self._ensure_token()
        return propfind(share_sync_base_url, token, client=self._http)

    def download_outputs(
        self,
        share_sync_base_url: str,
        local_dir: Path,
    ) -> list[Path]:
        """Download all top-level output files to *local_dir* (§9.2 / §9.3).

        Returns the list of local paths written. Skips collection (directory)
        entries; does not recurse into sub-directories (Depth:1 is flat).
        """
        token = self._ensure_token()
        base_url = share_sync_base_url.rstrip("/")
        entries = propfind(base_url, token, client=self._http)
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        written: list[Path] = []
        for entry in entries:
            if entry.is_collection:
                continue
            file_url = file_url_from_entry(base_url, entry)
            local_path = local_dir / entry.name
            log.info("Downloading %s -> %s", file_url, local_path)
            download_file(file_url, token, local_path, client=self._http)
            written.append(local_path)
        return written


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _opt(body: dict[str, Any], key: str, value: Any) -> None:
    """Add *key*: *value* to *body* only when value is not None."""
    if value is not None:
        body[key] = value


def _parse_retry_after(resp: httpx.Response) -> float | None:
    value = resp.headers.get("retry-after") or resp.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None
