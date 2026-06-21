"""Typer test CLI for Spark Fuse — maps to every documented API operation."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated, Optional

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.markup import escape
from rich.table import Table

from .client import SparkFuseClient
from .errors import AuthError, SparkFuseError
from .models import JobStatus, LogEvent, QueueStatusEvent, TruncatedEvent

app = typer.Typer(
    name="spark-fuse",
    help="Spark Fuse GPU compute CLI. Reads credentials from .env.",
    no_args_is_help=True,
)
console = Console()
err_console = Console(stderr=True)


@app.callback()
def _setup() -> None:
    """Load .env before every command."""
    load_dotenv()


def _client() -> SparkFuseClient:
    host = os.environ.get("SPARK_HOST", "").strip()
    email = os.environ.get("SPARK_EMAIL", "").strip()
    password = os.environ.get("SPARK_PASSWORD", "").strip()
    if not host:
        err_console.print("[red]SPARK_HOST not set. Add it to .env or export it.[/red]")
        raise typer.Exit(1)
    if not email or not password:
        err_console.print("[red]SPARK_EMAIL and SPARK_PASSWORD must be set.[/red]")
        raise typer.Exit(1)
    return SparkFuseClient(host=host, email=email, password=password)


@app.command()
def login() -> None:
    """Verify credentials and print token metadata (never the full token)."""
    with _client() as c:
        try:
            resp = c.login()
        except AuthError as e:
            err_console.print(f"[red]Login failed:[/red] {e.resp}")
            raise typer.Exit(1)
        prefix = (resp.token or "")[:20]
        console.print(f"[green]Login successful.[/green] Token prefix: [cyan]{prefix}...[/cyan]")
        if resp.password_expires_in_days is not None:
            console.print(f"Password expires in [yellow]{resp.password_expires_in_days}[/yellow] day(s).")
        if resp.password_expired:
            console.print("[yellow]Warning:[/yellow] password has expired.")
        if resp.requires_password_change:
            console.print("[yellow]Warning:[/yellow] password change required before meaningful work.")


@app.command()
def skus() -> None:
    """List eligible GPU instance types (SKUs)."""
    with _client() as c:
        c.login()
        sku_list = c.list_skus()
    table = Table(title="Available SKUs")
    table.add_column("Instance Type", style="cyan")
    import json
    for sku in sku_list:
        if isinstance(sku, dict):
            table.add_row(json.dumps(sku))
        else:
            table.add_row(str(sku))
    console.print(table)


@app.command()
def estimate(
    instance_type: Annotated[str, typer.Argument(help="Instance type SKU, e.g. g4dn.xlarge")],
    runtime: Annotated[Optional[int], typer.Option("--runtime", help="Estimated runtime (seconds)")] = None,
    idle_hold: Annotated[Optional[int], typer.Option("--idle-hold", help="Idle hold (seconds)")] = None,
    mode: Annotated[Optional[str], typer.Option("--mode", help="instant or smart")] = None,
) -> None:
    """Get a cost estimate without submitting a job."""
    with _client() as c:
        c.login()
        result = c.estimate(
            instance_type=instance_type,
            mode=mode,
            estimated_runtime_seconds=runtime,
            idle_hold_seconds=idle_hold,
        )
    console.print(f"[cyan]Instance type:[/cyan]  {result.instance_type}")
    console.print(f"[cyan]Mode:[/cyan]           {result.mode}")
    console.print(f"[cyan]Rate:[/cyan]           ${result.rate.billed_per_hour_usd}/hr")
    if result.estimate:
        console.print(f"[cyan]Billable seconds:[/cyan] {result.estimate.billable_seconds}")
        console.print(f"[cyan]Estimated total:[/cyan]  ${result.estimate.total_usd}")
    for note in result.notes:
        console.print(f"[yellow]Note:[/yellow] {note}")


@app.command()
def submit(
    image: Annotated[str, typer.Option("--image", "-i", help="Docker image reference")],
    command: Annotated[list[str], typer.Option("--command", "-c", help="Command tokens (pass once per token)")],
    instance_type: Annotated[str, typer.Option("--instance-type", "-t", help="SKU")],
    mode: Annotated[Optional[str], typer.Option("--mode", help="instant or smart")] = None,
    tag: Annotated[Optional[list[str]], typer.Option("--tag", help="Job tag (repeatable)")] = None,
    input_dir: Annotated[Optional[Path], typer.Option("--input-dir", help="Local dir to tar+upload (push workflow §3.1)")] = None,
    input_path: Annotated[Optional[str], typer.Option("--input-path", help="ShareSync path to mount (mount workflow §3.2)")] = None,
    input_space: Annotated[Optional[str], typer.Option("--input-space", help="ShareSync Project for --input-path (omit = Personal)")] = None,
    assets_path: Annotated[Optional[str], typer.Option("--assets-path", help="ShareSync path mounted read-only + lazy at /assets (model library §3.5)")] = None,
    assets_space: Annotated[Optional[str], typer.Option("--assets-space", help="ShareSync Project for --assets-path (omit = Personal)")] = None,
    image_affinity: Annotated[Optional[str], typer.Option("--image-affinity", help="preferred (default) or required (notify on cache miss)")] = None,
    instance_handle: Annotated[Optional[str], typer.Option("--instance-handle", help="Route onto a prepared warm session (§13); see 'spark-fuse instance prepare')")] = None,
    env: Annotated[Optional[list[str]], typer.Option("--env", help="Container env var KEY=VALUE (repeatable)")] = None,
) -> None:
    """Submit a compute job.

    Examples:

      spark-fuse submit --image alpine:3 --command echo --command hello --instance-type g4dn.xlarge

      spark-fuse submit --image pytorch/pytorch:2.7.1-cuda12.8-cudnn9-runtime \\
        --command bash -c "python3 /input/run.py" \\
        --instance-type g7e.2xlarge --input-dir ./my-scripts

      spark-fuse submit --image ghcr.io/org/comfyui@sha256:... \\
        --command python3.13 --command /runner/spark_fuse_run.py \\
        --instance-type g7e.2xlarge --input-path /jobs/klein/ \\
        --assets-path /comfy-flux2-klein/models --env MODEL_BASE_DIR=/assets \\
        --image-affinity required
    """
    push_mode = "auto-prepare" if input_dir is not None else None
    env_dict: Optional[dict[str, str]] = None
    if env:
        env_dict = {}
        for item in env:
            if "=" not in item:
                err_console.print(f"[red]Invalid --env '{item}'. Use KEY=VALUE.[/red]")
                raise typer.Exit(2)
            key, value = item.split("=", 1)
            env_dict[key] = value
    with _client() as c:
        c.login()
        resp = c.submit(
            image=image,
            command=command,
            instance_type=instance_type,
            mode=mode,
            tags=tag or None,
            env=env_dict,
            input_push_mode=push_mode,
            input_share_sync_path=input_path,
            input_share_sync_space_name=input_space,
            assets_share_sync_path=assets_path,
            assets_share_sync_space_name=assets_space,
            image_affinity=image_affinity,
            instance_handle=instance_handle,
        )
        console.print(f"[green]Job submitted:[/green] [cyan]{resp.job_id}[/cyan]")
        console.print(f"Status: {resp.status}")
        if resp.queue_position is not None:
            console.print(f"Queue position: {resp.queue_position}  ETA: {resp.estimated_start_seconds}s")
        console.print(f"Output path: {resp.output.share_sync_path}")

        if resp.input and resp.input.upload_url:
            if input_dir is not None:
                console.print(f"Uploading [cyan]{input_dir}[/cyan] → input...")
                c.upload_input(input_dir, resp.input.upload_url)
                console.print("[green]Input uploaded.[/green]")
            else:
                console.print(f"[yellow]Upload URL:[/yellow] {resp.input.upload_url}")
                console.print("  Run: spark-fuse upload-input <job-id> <local-dir>")


@app.command()
def status(
    job_id: Annotated[str, typer.Argument(help="Job ID (UUID)")],
) -> None:
    """Get the current status of a job."""
    with _client() as c:
        c.login()
        job = c.get_job(job_id)
    status_color = {
        "succeeded": "green",
        "failed": "red",
        "running": "yellow",
        "queued": "blue",
        "provisioning": "blue",
        "cancelled": "dim",
    }.get(job.status, "white")
    console.print(f"[cyan]Job:[/cyan]      {job.id}")
    console.print(f"[cyan]Status:[/cyan]   [{status_color}]{job.status}[/{status_color}]")
    if job.error_code:
        console.print(f"[red]Error:[/red]    {job.error_code}")
        if job.error_message:
            console.print(f"          {job.error_message}")
    if job.exit_code is not None:
        console.print(f"[cyan]Exit code:[/cyan] {job.exit_code}")
    console.print(f"[cyan]Image:[/cyan]    {job.image}")
    console.print(f"[cyan]Instance:[/cyan] {job.instance_type_name}")
    if job.gpu_name:
        console.print(f"[cyan]GPU:[/cyan]      {job.gpu_name}")
    if job.image_affinity:
        console.print(f"[cyan]Affinity:[/cyan] {job.image_affinity}")
    if job.image_cache_hit is not None:
        hit = "hit (image already cached)" if job.image_cache_hit else "miss (cold pull)"
        console.print(f"[cyan]Img cache:[/cyan] {hit}")
    console.print(f"[cyan]Created:[/cyan]  {job.created_at}")
    if job.terminal_at:
        console.print(f"[cyan]Terminal:[/cyan] {job.terminal_at}")
    if job.output.share_sync_base_url:
        console.print(f"[cyan]Outputs:[/cyan]  {job.output.share_sync_base_url}")


@app.command("list")
def list_jobs(
    tag: Annotated[Optional[list[str]], typer.Option("--tag", help="AND filter — job must carry every listed tag (repeatable)")] = None,
    tags_any: Annotated[Optional[str], typer.Option("--tags-any", help="OR filter — comma-separated; job must carry at least one")] = None,
) -> None:
    """List jobs for your organisation."""
    with _client() as c:
        c.login()
        jobs = c.list_jobs(tags=tag or None, tags_any=tags_any)
    table = Table(title=f"Jobs ({len(jobs)})")
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Status")
    table.add_column("Instance")
    table.add_column("Created")
    table.add_column("Image")
    for job in jobs:
        sc = {
            "succeeded": "green",
            "failed": "red",
            "running": "yellow",
            "queued": "blue",
            "provisioning": "blue",
            "cancelled": "dim",
        }.get(job.status, "white")
        table.add_row(
            job.id[:8] + "…",
            f"[{sc}]{job.status}[/{sc}]",
            job.instance_type_name,
            (job.created_at or "")[:19],
            (job.image or "")[:40],
        )
    console.print(table)


@app.command()
def logs(
    job_id: Annotated[str, typer.Argument(help="Job ID")],
) -> None:
    """Stream live logs for a job (SSE; no historical replay — connect right after submit)."""
    with _client() as c:
        c.login()
        try:
            for event in c.stream_logs(job_id):
                if isinstance(event, QueueStatusEvent):
                    if not event.done:
                        console.print(
                            f"[blue][queue][/blue] {event.status}  "
                            f"position={event.queue_position}  "
                            f"eta={event.estimated_start_seconds}s"
                        )
                    else:
                        console.print(f"[blue][queue][/blue] {event.status} — container starting")
                elif isinstance(event, LogEvent):
                    color = "green" if event.stream == "stdout" else "yellow"
                    # Escape the log line: container output can contain '[...]'
                    # (e.g. ComfyUI's '[/path/to/node]' lines) which Rich would
                    # otherwise parse as markup and raise MarkupError.
                    console.print(f"[{color}][{event.stream}][/{color}] {escape(event.line)}")
                elif isinstance(event, TruncatedEvent):
                    console.print(f"[red][truncated][/red] {escape(str(event.data))}")
        except KeyboardInterrupt:
            console.print("\n[yellow]Stream interrupted.[/yellow]")


@app.command()
def cancel(
    job_id: Annotated[str, typer.Argument(help="Job ID")],
) -> None:
    """Cancel a running or queued job (idempotent)."""
    with _client() as c:
        c.login()
        job = c.cancel(job_id)
    console.print(f"[yellow]Cancel requested.[/yellow] Current status: {job.status}")


@app.command()
def download(
    job_id: Annotated[str, typer.Argument(help="Job ID")],
    output_dir: Annotated[Path, typer.Argument(help="Local directory to save outputs into")],
) -> None:
    """Download all top-level output files for a completed job."""
    with _client() as c:
        c.login()
        job = c.get_job(job_id)
        base_url = job.output.share_sync_base_url
        if not base_url:
            err_console.print(
                "[red]No shareSyncBaseUrl on this job.[/red] "
                "Is the job completed? Run 'spark-fuse status <job-id>' first."
            )
            raise typer.Exit(1)
        console.print(f"Listing outputs at [cyan]{base_url}[/cyan]...")
        paths = c.download_outputs(base_url, output_dir)
    if paths:
        console.print(f"[green]Downloaded {len(paths)} file(s)[/green] → {output_dir}")
        for p in paths:
            console.print(f"  {p}")
    else:
        console.print("[yellow]No files found at that URL.[/yellow]")


instance_app = typer.Typer(help="Persistent-compute sessions: pre-warm an instance and run jobs back to back (§13).")
app.add_typer(instance_app, name="instance")


def _print_session(sess) -> None:
    colour = {"ready": "green", "running": "cyan", "preparing": "yellow"}.get(sess.status, "white")
    console.print(f"Handle: [cyan]{sess.instance_handle}[/cyan]")
    console.print(f"Status: [{colour}]{sess.status}[/{colour}]  ({sess.instance_type or '?'})")
    if sess.expires_at:
        console.print(f"Expires at: {sess.expires_at}")
    if sess.error_code:
        console.print(f"[red]Error:[/red] {sess.error_code} {sess.error_message or ''}")


@instance_app.command("prepare")
def instance_prepare(
    instance_type: Annotated[str, typer.Argument(help="Instance type SKU, e.g. g7e.2xlarge")],
    hold_seconds: Annotated[int, typer.Option("--hold-seconds", help="Max wall-clock to keep the instance held (clock starts at ready).")] = 1800,
) -> None:
    """Pre-warm an Instant-mode instance and print its session handle."""
    with _client() as c:
        c.login()
        sess = c.prepare_instance(instance_type=instance_type, hold_seconds=hold_seconds)
    _print_session(sess)


@instance_app.command("status")
def instance_status(
    handle: Annotated[str, typer.Argument(help="Session instance handle")],
) -> None:
    """Poll a session's current status."""
    with _client() as c:
        c.login()
        sess = c.get_instance(handle)
    _print_session(sess)


@instance_app.command("release")
def instance_release(
    handle: Annotated[str, typer.Argument(help="Session instance handle")],
) -> None:
    """Release a session (idempotent); the instance stops and billing ends."""
    with _client() as c:
        c.login()
        sess = c.release_instance(handle)
    console.print(f"[yellow]Release requested.[/yellow] Status: {sess.status}")


sharesync_app = typer.Typer(help="Resolve your ShareSync location and Projects (§3.4).")
app.add_typer(sharesync_app, name="sharesync")


@sharesync_app.command("location")
def sharesync_location() -> None:
    """Show your files host and Personal-space WebDAV base URL (no prior job needed)."""
    with _client() as c:
        c.login()
        loc = c.sharesync_location()
    console.print(f"[cyan]Files host:[/cyan] {loc.get('filesHost')}")
    personal = loc.get("personal") or {}
    console.print(f"[cyan]Personal:[/cyan]   {personal.get('webDavBaseUrl')}")


@sharesync_app.command("projects")
def sharesync_projects() -> None:
    """List your ShareSync Projects with their WebDAV base URLs."""
    with _client() as c:
        c.login()
        data = c.sharesync_projects()
    projects = data.get("projects") or []
    table = Table(title=f"ShareSync Projects ({len(projects)})")
    table.add_column("Name", style="cyan")
    table.add_column("WebDAV base URL")
    for proj in projects:
        table.add_row(proj.get("name", ""), proj.get("webDavBaseUrl", ""))
    console.print(table)
