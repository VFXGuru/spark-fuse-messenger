"""Spark Fuse — Python client library for the Spark Fuse GPU compute API."""
from .client import SparkFuseClient
from .errors import (
    AuthError,
    ForbiddenError,
    RateLimitError,
    ServiceUnavailableError,
    ShareSyncError,
    SparkFuseError,
    SparkHttpError,
    TokenExpiredError,
)
from .models import (
    CreateJobResponse,
    ErrorCode,
    EstimateResponse,
    Job,
    JobStatus,
    LoginResponse,
    LogEvent,
    QueueStatusEvent,
    TERMINAL_STATUSES,
    TruncatedEvent,
)

__all__ = [
    "SparkFuseClient",
    # errors
    "AuthError",
    "ForbiddenError",
    "RateLimitError",
    "ServiceUnavailableError",
    "ShareSyncError",
    "SparkFuseError",
    "SparkHttpError",
    "TokenExpiredError",
    # models
    "CreateJobResponse",
    "ErrorCode",
    "EstimateResponse",
    "Job",
    "JobStatus",
    "LoginResponse",
    "LogEvent",
    "QueueStatusEvent",
    "TERMINAL_STATUSES",
    "TruncatedEvent",
]
