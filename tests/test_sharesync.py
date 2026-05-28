"""Tests for sharesync.py — tarfile creation, PROPFIND parsing, download."""
from __future__ import annotations

import io
import tarfile
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from spark_fuse.errors import ShareSyncError
from spark_fuse.sharesync import (
    _parse_multistatus,
    download_file,
    file_url_from_entry,
    propfind,
    upload_directory,
)
from spark_fuse.models import ShareSyncEntry
from tests.conftest import mock_response

TOKEN = "fake-token"
BASE_URL = "https://org.files.sparkcloud.studio/dav/spaces/s1/Spark%20Fuse%20Jobs/abc/"


# ------------------------------------------------------------------
# upload_directory
# ------------------------------------------------------------------

def test_upload_directory_creates_tar_and_puts():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = Path(tmpdir)
        (d / "hello.txt").write_text("hello world")
        (d / "sub").mkdir()
        (d / "sub" / "data.bin").write_bytes(b"\x00\x01\x02")

        http = MagicMock(spec=httpx.Client)
        http.put.return_value = mock_response(204)

        upload_directory(d, "https://files.example.com/upload.tar.gz", TOKEN, client=http)

        http.put.assert_called_once()
        call_kwargs = http.put.call_args
        assert call_kwargs[0][0] == "https://files.example.com/upload.tar.gz"
        assert call_kwargs[1]["headers"]["Authorization"] == f"Bearer {TOKEN}"

        # Verify the uploaded bytes are a valid tar.gz with correct members
        content = call_kwargs[1]["content"]
        buf = io.BytesIO(content)
        with tarfile.open(fileobj=buf, mode="r:gz") as tf:
            names = tf.getnames()
        assert "hello.txt" in names
        assert "sub/data.bin" in names
        # No absolute paths or parent path components
        assert all(not n.startswith("/") for n in names)


def test_upload_directory_missing_dir_raises():
    http = MagicMock(spec=httpx.Client)
    with pytest.raises(ShareSyncError, match="does not exist"):
        upload_directory(Path("/nonexistent/dir"), "https://x.com/up", TOKEN, client=http)


def test_upload_directory_non_200_raises():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = Path(tmpdir)
        (d / "f.txt").write_text("x")
        http = MagicMock(spec=httpx.Client)
        http.put.return_value = mock_response(500, text="error")
        with pytest.raises(ShareSyncError, match="HTTP 500"):
            upload_directory(d, "https://x.com/up", TOKEN, client=http)


# ------------------------------------------------------------------
# _parse_multistatus
# ------------------------------------------------------------------

MULTISTATUS_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:">
  <d:response>
    <d:href>/dav/spaces/s1/Spark%20Fuse%20Jobs/abc/</d:href>
    <d:propstat>
      <d:prop>
        <d:resourcetype><d:collection/></d:resourcetype>
        <d:displayname>abc</d:displayname>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/dav/spaces/s1/Spark%20Fuse%20Jobs/abc/results.tar.gz</d:href>
    <d:propstat>
      <d:prop>
        <d:resourcetype/>
        <d:displayname>results.tar.gz</d:displayname>
        <d:getcontentlength>12345</d:getcontentlength>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/dav/spaces/s1/Spark%20Fuse%20Jobs/abc/spark-fuse-abc.log</d:href>
    <d:propstat>
      <d:prop>
        <d:resourcetype/>
        <d:getcontentlength>999</d:getcontentlength>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
</d:multistatus>
"""


def test_parse_multistatus_finds_collection_and_files():
    entries = _parse_multistatus(MULTISTATUS_XML)
    assert len(entries) == 3
    collections = [e for e in entries if e.is_collection]
    files = [e for e in entries if not e.is_collection]
    assert len(collections) == 1
    assert len(files) == 2


def test_parse_multistatus_file_names():
    entries = _parse_multistatus(MULTISTATUS_XML)
    names = {e.name for e in entries if not e.is_collection}
    assert "results.tar.gz" in names
    assert "spark-fuse-abc.log" in names


def test_parse_multistatus_content_length():
    entries = _parse_multistatus(MULTISTATUS_XML)
    results = next(e for e in entries if e.name == "results.tar.gz")
    assert results.content_length == 12345


def test_parse_multistatus_invalid_xml_raises():
    with pytest.raises(ShareSyncError, match="Could not parse"):
        _parse_multistatus("<not valid xml")


# ------------------------------------------------------------------
# propfind
# ------------------------------------------------------------------

def test_propfind_sends_propfind_with_depth():
    http = MagicMock(spec=httpx.Client)
    http.request.return_value = mock_response(207, text=MULTISTATUS_XML)
    http.request.return_value.status_code = 207
    http.request.return_value.text = MULTISTATUS_XML

    entries = propfind(BASE_URL, TOKEN, client=http)
    assert len(entries) == 3
    call_args = http.request.call_args
    assert call_args[0][0] == "PROPFIND"
    assert call_args[1]["headers"]["Depth"] == "1"
    assert call_args[1]["headers"]["Authorization"] == f"Bearer {TOKEN}"


def test_propfind_non_207_raises():
    http = MagicMock(spec=httpx.Client)
    http.request.return_value = mock_response(403, text="Forbidden")
    with pytest.raises(ShareSyncError, match="HTTP 403"):
        propfind(BASE_URL, TOKEN, client=http)


# ------------------------------------------------------------------
# file_url_from_entry
# ------------------------------------------------------------------

def test_file_url_from_entry():
    entry = ShareSyncEntry(
        href="/dav/spaces/s1/Spark%20Fuse%20Jobs/abc/results.tar.gz",
        name="results.tar.gz",
        is_collection=False,
        content_length=100,
    )
    url = file_url_from_entry(BASE_URL, entry)
    assert url == "https://org.files.sparkcloud.studio/dav/spaces/s1/Spark%20Fuse%20Jobs/abc/results.tar.gz"


# ------------------------------------------------------------------
# download_file
# ------------------------------------------------------------------

def test_download_file_writes_content(tmp_path):
    http = MagicMock(spec=httpx.Client)
    file_content = b"output data here"

    # Mock streaming context manager
    mock_stream_resp = MagicMock()
    mock_stream_resp.status_code = 200
    mock_stream_resp.iter_bytes.return_value = iter([file_content])
    mock_stream_resp.__enter__ = MagicMock(return_value=mock_stream_resp)
    mock_stream_resp.__exit__ = MagicMock(return_value=False)
    http.stream.return_value = mock_stream_resp

    out = tmp_path / "result.bin"
    download_file("https://x.com/file.bin", TOKEN, out, client=http)

    assert out.read_bytes() == file_content


def test_download_file_non_200_raises(tmp_path):
    http = MagicMock(spec=httpx.Client)
    mock_stream_resp = MagicMock()
    mock_stream_resp.status_code = 404
    mock_stream_resp.__enter__ = MagicMock(return_value=mock_stream_resp)
    mock_stream_resp.__exit__ = MagicMock(return_value=False)
    http.stream.return_value = mock_stream_resp

    with pytest.raises(ShareSyncError, match="HTTP 404"):
        download_file("https://x.com/missing.bin", TOKEN, tmp_path / "out", client=http)
