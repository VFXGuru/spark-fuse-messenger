"""SSE log streaming for Spark Fuse jobs (§4).

Stream starts from the moment of connection — no historical replay.
Connect immediately after submit to avoid missing early log lines.

SSE keepalive comment lines (starting with ':') are handled transparently
by the httpx-sse parser and are never yielded here.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Generator
from typing import Union

import httpx
import httpx_sse

from .models import LogEvent, QueueStatusEvent, TruncatedEvent

log = logging.getLogger(__name__)

SSEEvent = Union[QueueStatusEvent, LogEvent, TruncatedEvent]


def stream_logs(
    url: str,
    token: str,
    *,
    client: httpx.Client,
) -> Generator[SSEEvent, None, None]:
    """Connect to the SSE log stream at *url* and yield parsed events.

    Yields:
        QueueStatusEvent  — job is queued/provisioning (done=True when running)
        LogEvent          — a container log line (stdout or stderr)
        TruncatedEvent    — server dropped some lines

    The generator exhausts when the server closes the stream (job terminal).
    Unknown event types are logged at DEBUG and skipped.
    """
    with httpx_sse.connect_sse(
        client,
        "GET",
        url,
        headers={"Authorization": f"Bearer {token}"},
    ) as event_source:
        for sse in event_source.iter_sse():
            event_type = sse.event
            if not event_type or not sse.data:
                continue
            try:
                data = json.loads(sse.data)
            except json.JSONDecodeError:
                log.warning("Unparseable SSE data (event=%r): %s", event_type, sse.data[:200])
                continue

            if event_type == "queue.status":
                yield QueueStatusEvent.from_dict(data)
            elif event_type == "log":
                yield LogEvent.from_dict(data, event_id=sse.id)
            elif event_type == "truncated":
                yield TruncatedEvent(data=data)
            else:
                log.debug("Unknown SSE event type %r; skipping", event_type)
