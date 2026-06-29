"""Dataclasses and enums for Spark Fuse API responses."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROVISIONING = "provisioning"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SMARTCOMPUTE_INTERRUPTED = "smartcompute-interrupted"


TERMINAL_STATUSES: frozenset[str] = frozenset({
    JobStatus.SUCCEEDED,
    JobStatus.FAILED,
    JobStatus.CANCELLED,
    JobStatus.SMARTCOMPUTE_INTERRUPTED,
})


class ErrorCode(str, Enum):
    IMAGE_PULL_FAILED = "image_pull_failed"
    DISK_FULL = "disk_full"
    INPUT_DOWNLOAD_FAILED = "input_download_failed"
    OUTPUT_UPLOAD_FAILED = "output_upload_failed"
    OUTPUT_MOUNT_FAILED = "output_mount_failed"
    OUTPUT_DRAIN_FAILED = "output_drain_failed"
    OUTPUT_UNMOUNT_FAILED = "output_unmount_failed"
    CONTAINER_NONZERO_EXIT = "container_nonzero_exit"
    CANCELLED = "cancelled"
    AGENT_SILENT = "agent_silent"
    START_TIMEOUT = "start_timeout"
    NO_CAPACITY = "no_capacity"
    SMARTCOMPUTE_INTERRUPTED = "smartcompute_interrupted"
    SMARTCOMPUTE_INTERRUPTED_NO_RETRIES_LEFT = "smartcompute_interrupted_no_retries_left"
    INSTANCE_HANDLE_INVALID = "instance_handle_invalid"
    WALLCLOCK_EXCEEDED = "wallclock_exceeded"
    CONTAINER_INACTIVE = "container_inactive"
    # v1.24 queue error codes (§13.6)
    QUEUE_HALTED_ON_FAILURE = "queue_halted_on_failure"
    QUEUE_RELEASED = "queue_released"
    SESSION_MAX_DURATION_EXCEEDED = "session_max_duration_exceeded"


@dataclass
class LoginResponse:
    success: bool
    token: str | None
    resp: str
    password_expired: bool
    password_expires_in_days: int | None
    requires_password_change: bool

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LoginResponse":
        return cls(
            success=data["success"],
            token=data.get("token"),
            resp=data.get("resp", ""),
            password_expired=data.get("password_expired", False),
            password_expires_in_days=data.get("password_expires_in_days"),
            requires_password_change=data.get("requires_password_change", False),
        )


@dataclass
class ShareSyncOutput:
    share_sync_path: str
    share_sync_space_name: str | None
    # null in list responses; present on get_job() — see §6 note
    share_sync_base_url: str | None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ShareSyncOutput":
        return cls(
            share_sync_path=data.get("shareSyncPath") or data.get("share_sync_path", ""),
            share_sync_space_name=data.get("shareSyncSpaceName") or data.get("share_sync_space_name"),
            share_sync_base_url=data.get("shareSyncBaseUrl") or data.get("share_sync_base_url"),
        )


@dataclass
class ShareSyncInput:
    share_sync_path: str
    share_sync_space_name: str | None
    share_sync_base_url: str | None
    # Present only when inputPushMode='auto-prepare'
    upload_url: str | None = None
    upload_method: str | None = None
    example_curl: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ShareSyncInput":
        return cls(
            share_sync_path=data.get("shareSyncPath") or data.get("share_sync_path", ""),
            share_sync_space_name=data.get("shareSyncSpaceName") or data.get("share_sync_space_name"),
            share_sync_base_url=data.get("shareSyncBaseUrl") or data.get("share_sync_base_url"),
            upload_url=data.get("uploadUrl"),
            upload_method=data.get("uploadMethod"),
            example_curl=data.get("exampleCurl"),
        )


@dataclass
class CreateJobResponse:
    """Response shape from POST /api/compute/jobs (§12.4 CreateComputeJobResponse)."""

    job_id: str
    status: str
    image_digest: str | None
    output_share_sync_path: str
    created_at: str
    output: ShareSyncOutput
    input: ShareSyncInput | None
    queue_position: int | None = None
    estimated_start_seconds: int | None = None
    shm_size: str | None = None
    notify_on_failure: bool | None = None
    max_wall_clock_seconds: int | None = None
    container_inactivity_seconds: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CreateJobResponse":
        inp = data.get("input")
        return cls(
            job_id=data["jobId"],
            status=data["status"],
            image_digest=data.get("imageDigest"),
            output_share_sync_path=data["outputShareSyncPath"],
            created_at=data["createdAt"],
            output=ShareSyncOutput.from_dict(data["output"]),
            input=ShareSyncInput.from_dict(inp) if inp else None,
            queue_position=data.get("queuePosition"),
            estimated_start_seconds=data.get("estimatedStartSeconds"),
            shm_size=data.get("shmSize"),
            notify_on_failure=data.get("notifyOnFailure"),
            max_wall_clock_seconds=data.get("maxWallClockSeconds"),
            container_inactivity_seconds=data.get("containerInactivitySeconds"),
        )


@dataclass
class Job:
    """Full job row from GET /api/compute/jobs/:id (§12.5 ComputeJobApiShape).

    The API returns fields in snake_case with some camelCase aliases; we normalise
    to snake_case here.
    """

    id: str
    image: str
    command: list[str]
    instance_type_name: str
    mode: str
    status: str
    error_code: str | None
    error_message: str | None
    exit_code: int | None
    created_at: str
    output: ShareSyncOutput
    input: ShareSyncInput | None
    organisation_id: int | None = None
    user_id: int | None = None
    image_digest: str | None = None
    image_cache_hit: bool | None = None
    image_affinity: str | None = None
    started_provisioning_at: str | None = None
    started_running_at: str | None = None
    terminal_at: str | None = None
    cancel_requested_at: str | None = None
    cuda_version: str | None = None
    driver_version: str | None = None
    gpu_name: str | None = None
    log_archive_share_sync_path: str | None = None
    log_archive_uploaded_at: str | None = None
    idle_hold_seconds: int | None = None
    shm_size: str | None = None
    notify_on_failure: bool | None = None
    max_wall_clock_seconds: int | None = None
    container_inactivity_seconds: int | None = None
    queue_position: int | None = None
    estimated_start_seconds: int | None = None
    max_retries_on_interrupt: int | None = None
    retries_used_on_interrupt: int | None = None
    tags: list[str] = field(default_factory=list)
    # v1.24 queue fields — set when this job was submitted via a prepared-instance queue
    instance_queue_position: int | None = None
    instance_queue_state: str | None = None  # "dispatched" | "pending" | None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Job":
        out = data.get("output") or {}
        inp = data.get("input")
        return cls(
            id=data["id"],
            image=data["image"],
            command=data.get("command") or [],
            instance_type_name=data.get("instance_type_name", ""),
            mode=data.get("mode", "instant"),
            status=data["status"],
            error_code=data.get("error_code"),
            error_message=data.get("error_message"),
            exit_code=data.get("exit_code"),
            created_at=data.get("created_at", ""),
            output=ShareSyncOutput.from_dict(out) if out else ShareSyncOutput("", None, None),
            input=ShareSyncInput.from_dict(inp) if inp else None,
            organisation_id=data.get("organisation_id"),
            user_id=data.get("user_id"),
            image_digest=data.get("image_digest"),
            # imageCacheHit can be False (cold pull), so default-get rather than `or`
            image_cache_hit=data.get("imageCacheHit", data.get("image_cache_hit")),
            image_affinity=data.get("imageAffinity") or data.get("image_affinity"),
            started_provisioning_at=data.get("started_provisioning_at"),
            started_running_at=data.get("started_running_at"),
            terminal_at=data.get("terminal_at"),
            cancel_requested_at=data.get("cancel_requested_at"),
            cuda_version=data.get("cuda_version"),
            driver_version=data.get("driver_version"),
            gpu_name=data.get("gpu_name"),
            log_archive_share_sync_path=data.get("log_archive_share_sync_path"),
            log_archive_uploaded_at=data.get("log_archive_uploaded_at"),
            idle_hold_seconds=data.get("idle_hold_seconds"),
            # Both snake_case and camelCase aliases are present on job rows
            shm_size=data.get("container_shm_size") or data.get("shmSize"),
            notify_on_failure=data.get("notify_on_failure"),
            max_wall_clock_seconds=data.get("max_wallclock_seconds") or data.get("maxWallClockSeconds"),
            container_inactivity_seconds=data.get("container_inactivity_seconds") or data.get("containerInactivitySeconds"),
            queue_position=data.get("queuePosition") or data.get("queue_position"),
            estimated_start_seconds=data.get("estimatedStartSeconds") or data.get("estimated_start_seconds"),
            max_retries_on_interrupt=data.get("max_retries_on_interrupt"),
            retries_used_on_interrupt=data.get("retries_used_on_interrupt"),
            tags=data.get("tags") or [],
            instance_queue_position=data.get("instanceQueuePosition") or data.get("instance_queue_position"),
            instance_queue_state=data.get("instanceQueueState") or data.get("instance_queue_state"),
        )

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES


SESSION_TERMINAL_STATUSES: frozenset[str] = frozenset({"released", "expired", "failed"})


@dataclass
class PreparedInstance:
    """A persistent-compute session (§13).

    Same shape is returned by POST /api/compute/instances/prepare,
    GET /api/compute/instances/{handle}, and the release endpoint. Statuses:
    preparing -> ready -> running -> ready ... then a terminal released/expired/failed.
    """

    instance_handle: str
    status: str
    instance_type: str | None = None
    hold_seconds: int | None = None
    prepared_at: str | None = None
    ready_at: str | None = None
    released_at: str | None = None
    expired_at: str | None = None
    failed_at: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    expires_at: str | None = None
    first_job_id: str | None = None
    last_job_id: str | None = None
    # v1.24 queue fields (§13.6)
    halt_on_failure: bool | None = None
    queue: "InstanceQueue | None" = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PreparedInstance":
        queue_raw = data.get("queue")
        return cls(
            instance_handle=data.get("instanceHandle") or data.get("instance_handle", ""),
            status=data.get("status", ""),
            instance_type=data.get("instanceType") or data.get("instance_type"),
            hold_seconds=data.get("holdSeconds") if data.get("holdSeconds") is not None else data.get("hold_seconds"),
            prepared_at=data.get("preparedAt") or data.get("prepared_at"),
            ready_at=data.get("readyAt") or data.get("ready_at"),
            released_at=data.get("releasedAt") or data.get("released_at"),
            expired_at=data.get("expiredAt") or data.get("expired_at"),
            failed_at=data.get("failedAt") or data.get("failed_at"),
            error_code=data.get("errorCode") or data.get("error_code"),
            error_message=data.get("errorMessage") or data.get("error_message"),
            expires_at=data.get("expiresAt") or data.get("expires_at"),
            first_job_id=data.get("firstJobId") or data.get("first_job_id"),
            last_job_id=data.get("lastJobId") or data.get("last_job_id"),
            halt_on_failure=data.get("haltOnFailure"),
            queue=InstanceQueue.from_dict(queue_raw) if queue_raw else None,
        )

    @property
    def is_ready(self) -> bool:
        return self.status == "ready"

    @property
    def is_terminal(self) -> bool:
        return self.status in SESSION_TERMINAL_STATUSES


@dataclass
class EstimateRate:
    billed_per_second_cents: str
    billed_per_hour_usd: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EstimateRate":
        return cls(
            billed_per_second_cents=data["billedPerSecondCents"],
            billed_per_hour_usd=data["billedPerHourUsd"],
        )


@dataclass
class EstimateTotal:
    billable_seconds: int
    total_cents: str
    total_usd: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EstimateTotal":
        return cls(
            billable_seconds=data["billableSeconds"],
            total_cents=data["totalCents"],
            total_usd=data["totalUsd"],
        )


@dataclass
class EstimateResponse:
    """Response from POST /api/compute/jobs/estimate (§2.8)."""

    instance_type: str
    mode: str
    rate: EstimateRate
    estimate: EstimateTotal | None
    notes: list[str]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EstimateResponse":
        est = data.get("estimate")
        return cls(
            instance_type=data["instanceType"],
            mode=data["mode"],
            rate=EstimateRate.from_dict(data["rate"]),
            estimate=EstimateTotal.from_dict(est) if est else None,
            notes=data.get("notes", []),
        )


@dataclass
class QueueStatusEvent:
    """SSE event type 'queue.status' — emitted while job is queued/provisioning."""

    status: str
    queue_position: int | None
    estimated_start_seconds: int | None
    done: bool  # true when the job transitions to running or terminal

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueStatusEvent":
        return cls(
            status=data["status"],
            queue_position=data.get("queuePosition"),
            estimated_start_seconds=data.get("estimatedStartSeconds"),
            done=data.get("done", False),
        )


@dataclass
class LogEvent:
    """SSE event type 'log' — a single container log line."""

    ts: str
    stream: str   # "stdout" or "stderr"
    line: str
    phase: str    # "container" or provisioning phases
    event_id: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any], event_id: str | None = None) -> "LogEvent":
        return cls(
            ts=data["ts"],
            stream=data["stream"],
            line=data["line"],
            phase=data.get("phase", "container"),
            event_id=event_id,
        )


@dataclass
class TruncatedEvent:
    """SSE event type 'truncated' — server dropped some log lines."""

    data: dict[str, Any]


@dataclass
class QueuedJobRef:
    """One accepted job entry in an AppendJobsResponse (§13.6)."""

    job_id: str
    queue_position: int | None
    queue_state: str   # "dispatched" | "pending"
    status: str        # always "queued" at accept time

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueuedJobRef":
        return cls(
            job_id=data.get("jobId", ""),
            queue_position=data.get("queuePosition"),
            queue_state=data.get("queueState", ""),
            status=data.get("status", "queued"),
        )


@dataclass
class InstanceQueueEntry:
    """One job listed in an InstanceQueue (running / pending / completed slot)."""

    job_id: str
    position: int | None
    status: str | None = None   # only populated for completed entries

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InstanceQueueEntry":
        return cls(
            job_id=data.get("jobId", ""),
            position=data.get("position"),
            status=data.get("status"),
        )


@dataclass
class InstanceQueue:
    """Queue state returned by GET /api/compute/instances/:handle (§13.6)."""

    depth: int
    running: InstanceQueueEntry | None
    pending: list[InstanceQueueEntry]
    completed: list[InstanceQueueEntry]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InstanceQueue":
        running_raw = data.get("running")
        return cls(
            depth=data.get("depth", 0),
            running=InstanceQueueEntry.from_dict(running_raw) if running_raw else None,
            pending=[InstanceQueueEntry.from_dict(e) for e in data.get("pending", [])],
            completed=[InstanceQueueEntry.from_dict(e) for e in data.get("completed", [])],
        )


@dataclass
class AppendJobsResponse:
    """Response from POST /api/compute/instances/:handle/jobs (§13.6)."""

    instance_handle: str
    accepted: list[QueuedJobRef]
    queue_depth: int
    halt_on_failure: bool

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppendJobsResponse":
        return cls(
            instance_handle=data.get("instanceHandle", ""),
            accepted=[QueuedJobRef.from_dict(j) for j in data.get("accepted", [])],
            queue_depth=data.get("queueDepth", 0),
            halt_on_failure=data.get("haltOnFailure", False),
        )


@dataclass
class ShareSyncEntry:
    """One entry from a WebDAV PROPFIND multistatus response."""

    href: str
    name: str
    is_collection: bool
    content_length: int | None = None
