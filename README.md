# Spark Fuse Messenger

Python client and CLI for the [Spark Fuse](https://sparkcloud.studio) on-demand GPU compute API.

Submit Docker-image jobs to cloud GPUs, stream live logs, and pull outputs back from
ShareSync — all from Python or the command line.

## Requirements

- Python 3.12+
- A Spark Fuse account with API credentials

## Installation

Install from PyPI:

```powershell
pip install spark-fuse-messenger
```

Or clone and run from source with [uv](https://docs.astral.sh/uv/):

```powershell
git clone https://github.com/VFXGuru/spark-fuse-messenger
cd spark-fuse-messenger
uv sync
```

## API reference

This client implements the Spark Fuse REST API. The API is documented in
`spark-fuse-api-v124.md`, a file provided by Spark Cloud Studio to its customers.
That document is **not redistributed in this repo**. If you want to reference it
locally, place your own copy in the project root.

Copy `.env.example` to `.env` and fill in your credentials:

```
SPARK_HOST=https://api.prod.aapse1.sparkcloud.studio
SPARK_EMAIL=you@yourcompany.com
SPARK_PASSWORD=your-password
```

`.env` is gitignored and never committed.

Activate the venv or prefix every command with `uv run`:

```powershell
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # macOS / Linux
```

## CLI commands

```powershell
# Verify credentials (free)
spark-fuse login

# List available GPU SKUs (free)
spark-fuse skus

# Cost estimate — rate only, or with runtime (free)
spark-fuse estimate g4dn.xlarge --runtime 3600

# Submit a job
spark-fuse submit --image alpine:3 --command echo --command hello --instance-type g4dn.xlarge

# Submit with a local input directory (auto tar+upload)
spark-fuse submit --image pytorch/pytorch:2.7.1-cuda12.8-cudnn9-runtime \
  --command python3 --command /input/run.py \
  --instance-type g7e.2xlarge --input-dir ./my-scripts

# Poll status
spark-fuse status <job-id>

# List all jobs (optional tag filters)
spark-fuse list
spark-fuse list --tag ci --tag training          # AND filter
spark-fuse list --tags-any "ci,staging"          # OR filter

# Stream live logs (connect immediately after submit — no replay)
spark-fuse logs <job-id>

# Cancel (idempotent)
spark-fuse cancel <job-id>

# Download all output files
spark-fuse download <job-id> ./outputs

# Persistent-compute sessions: pre-warm one instance and run jobs back to back (§13)
spark-fuse instance prepare g7e.2xlarge --hold-seconds 1800   # prints a handle
spark-fuse instance status <handle>                           # poll until 'ready'
spark-fuse instance release <handle>                          # tear down
```

Submit jobs onto a prepared session by passing the handle: `spark-fuse submit
... --instance-handle <handle>` (or `instance_handle=` on `client.submit`). Each
job lands on that same warm instance with no cold start.

## Python API

```python
from spark_fuse import SparkFuseClient

with SparkFuseClient(host="...", email="...", password="...") as client:
    client.login()

    resp = client.submit(
        image="alpine:3",
        command=["echo", "hello"],
        instance_type="g4dn.xlarge",
    )
    job_id = resp.job_id

    for event in client.stream_logs(job_id):
        print(event)

    job = client.get_job(job_id)
    print(job.status, job.exit_code)
```

## Sessions (warm-instance pool)

Pre-warm a single instance and route several jobs to it so each job skips the
cold start and image pull. The `hold_seconds` clock starts when the instance
becomes `ready` and re-arms after each job, so it is an idle ceiling, not a
total-session ceiling.

```python
from spark_fuse import SparkFuseClient

with SparkFuseClient(host="...", email="...", password="...") as client:
    client.login()

    # context manager: prepare -> wait_until_ready -> yield -> release
    with client.session(instance_type="g7e.2xlarge", hold_seconds=600) as sess:
        handle = sess.instance_handle
        for workflow in my_workflows:
            resp = client.submit(..., instance_handle=handle)
            # poll resp.job_id to completion as normal
```

Or manage the lifecycle manually:

```python
sess = client.prepare_instance(instance_type="g7e.2xlarge", hold_seconds=600)
sess = client.wait_until_ready(sess.instance_handle)   # polls until ready
# ... submit jobs with instance_handle=sess.instance_handle ...
client.release_instance(sess.instance_handle)
```

**Error types** — `from spark_fuse import ...`:

| Exception | When raised |
|---|---|
| `NoWarmPoolCapacityError` | `prepare_instance` returns HTTP 503 (no warm pool slot available) |
| `SessionFailedError` | Instance reaches terminal `failed` status |
| `SessionNotFoundError` | `get_instance` or `release_instance` returns HTTP 404 |
| `SessionConflictError` | `release_instance` returns HTTP 409 (state conflict) |
| `SessionError` | Base class for all of the above |

**Session affinity in the ComfyUI bridge** — the bridge's render queue accepts a
`session_affinity` setting (`"preferred"` or `"required"`). With `"preferred"`
(the default), the queue retries on `NoWarmPoolCapacityError` and falls back to
independent submits if no slot becomes available. With `"required"`, it aborts
the queue instead of falling back. An explicit `"off"` mode (always independent
submits, never attempt prepare) is a known future toggle, not yet implemented.

## Running tests

All tests mock HTTP — no network calls required:

```powershell
uv run pytest -v
```

## Project structure

```
src/spark_fuse/
├── client.py      — SparkFuseClient (all API methods)
├── auth.py        — login + token management
├── sharesync.py   — WebDAV: tar+upload, PROPFIND, streaming download
├── logs.py        — SSE log streaming
├── errors.py      — typed exceptions
├── models.py      — dataclasses + enums (JobStatus, ErrorCode, …)
└── cli.py         — Typer CLI

tests/             — unit tests (HTTP mocked)
```

## License

[MIT](LICENSE) — Copyright (c) 2026 VFXGuru
