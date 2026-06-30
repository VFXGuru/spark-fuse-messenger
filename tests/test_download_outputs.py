"""Tests for download_outputs: subdirectory recursion, flattening, and collision guard.

Patches spark_fuse.client.propfind and spark_fuse.client.download_file directly
so tests focus on the new logic rather than the HTTP layer (which is covered by
test_sharesync.py). The mock download_file does NOT write files to disk; where
collision detection needs a pre-existing file it is created explicitly before
calling download_outputs.
"""
from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from spark_fuse.models import ShareSyncEntry
from tests.conftest import LOGIN_OK, make_client, mock_response

BASE_URL = "https://files.example.com/dav/spaces/s1/jobs/job-abc"


def _entry(name: str, href: str, is_collection: bool, size: int | None = None) -> ShareSyncEntry:
    return ShareSyncEntry(href=href, name=name, is_collection=is_collection,
                          content_length=size)


def _authed(http: MagicMock):
    http.post.return_value = mock_response(200, LOGIN_OK)
    c = make_client(http)
    c.login()
    return c


# ── test 1: nested subdir → all files download flat ──────────────────────────

def test_nested_subdir_downloads_all_files_flat(tmp_path):
    """Root: 1 log + 1 portraits/ subdir with 2 PNGs.
    download_outputs must return all 3 files, all in tmp_path root."""
    root_entries = [
        _entry("job-abc",          "/dav/spaces/s1/jobs/job-abc/",                   True),
        _entry("job.log",          "/dav/spaces/s1/jobs/job-abc/job.log",            False, 28_000),
        _entry("portraits",        "/dav/spaces/s1/jobs/job-abc/portraits/",          True),
    ]
    portraits_entries = [
        _entry("portraits",        "/dav/spaces/s1/jobs/job-abc/portraits/",          True),
        _entry("img_00001_.png",   "/dav/spaces/s1/jobs/job-abc/portraits/img_00001_.png", False, 3_600_000),
        _entry("img_00002_.png",   "/dav/spaces/s1/jobs/job-abc/portraits/img_00002_.png", False, 3_700_000),
    ]

    http = MagicMock(spec=httpx.Client)
    c = _authed(http)

    with patch("spark_fuse.client.propfind", side_effect=[root_entries, portraits_entries]), \
         patch("spark_fuse.client.download_file"):
        result = c.download_outputs(BASE_URL, tmp_path)

    assert len(result) == 3
    names = {p.name for p in result}
    assert names == {"job.log", "img_00001_.png", "img_00002_.png"}
    # All files are in tmp_path root — not in a portraits/ subfolder
    for p in result:
        assert p.parent == tmp_path


# ── test 2: flat structure (files at root, no subdir) ────────────────────────

def test_flat_structure_behaves_as_before(tmp_path):
    """Regression guard: flat layout (all files at root, no subdirectories).
    Should behave identically to the old Depth:1-only implementation."""
    root_entries = [
        _entry("job-abc",          "/dav/spaces/s1/jobs/job-abc/",              True),
        _entry("result.png",       "/dav/spaces/s1/jobs/job-abc/result.png",    False, 1_000_000),
        _entry("spark-fuse.log",   "/dav/spaces/s1/jobs/job-abc/spark-fuse.log", False, 5_000),
    ]

    http = MagicMock(spec=httpx.Client)
    c = _authed(http)

    with patch("spark_fuse.client.propfind", return_value=root_entries) as mock_pf, \
         patch("spark_fuse.client.download_file"):
        result = c.download_outputs(BASE_URL, tmp_path)

    # Only one PROPFIND call — no recursion into subdirs
    assert mock_pf.call_count == 1
    assert len(result) == 2
    names = {p.name for p in result}
    assert names == {"result.png", "spark-fuse.log"}
    for p in result:
        assert p.parent == tmp_path


# ── test 3: basename collision → no silent overwrite ─────────────────────────

def test_basename_collision_renames_without_overwrite(tmp_path, caplog):
    """If a file with the target basename already exists in local_dir (e.g. from a
    previous download or a same-run collision), the new file gets a _N suffix and
    a warning is logged — no silent overwrite."""
    # Pre-create the file that would collide
    (tmp_path / "img_00001_.png").write_bytes(b"existing content")

    root_entries = [
        _entry("job-abc",        "/dav/spaces/s1/jobs/job-abc/",                     True),
        _entry("img_00001_.png", "/dav/spaces/s1/jobs/job-abc/img_00001_.png", False, 100),
    ]

    http = MagicMock(spec=httpx.Client)
    c = _authed(http)

    with patch("spark_fuse.client.propfind", return_value=root_entries), \
         patch("spark_fuse.client.download_file"), \
         caplog.at_level(logging.WARNING, logger="spark_fuse.client"):
        result = c.download_outputs(BASE_URL, tmp_path)

    assert len(result) == 1
    # Must NOT overwrite — renamed to _1 suffix
    assert result[0].name == "img_00001__1.png"
    assert result[0].parent == tmp_path
    # Original file must be untouched (download_file is mocked, so it wrote nothing)
    assert (tmp_path / "img_00001_.png").read_bytes() == b"existing content"
    # Warning was logged
    assert any("collision" in r.message.lower() for r in caplog.records)
