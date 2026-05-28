"""WebDAV operations against ShareSync (§9).

ShareSync is authenticated with the same bearer token as the compute API —
no separate login needed (§1.2).
"""
from __future__ import annotations

import io
import logging
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx

from .errors import ShareSyncError
from .models import ShareSyncEntry

log = logging.getLogger(__name__)

_DAV_NS = "DAV:"


def _dav(tag: str) -> str:
    return f"{{{_DAV_NS}}}{tag}"


def upload_directory(
    local_dir: Path,
    upload_url: str,
    token: str,
    *,
    client: httpx.Client,
) -> None:
    """Build an in-memory tar.gz from *local_dir* and PUT it to *upload_url*.

    Members are stored relative to the directory root (no leading path
    components), matching the layout the server extracts into /input/ (§3.1).
    Uses stdlib tarfile — no external tar binary required (runs on Windows).
    The bearer token is sent in the Authorization header.
    """
    local_dir = local_dir.resolve()
    if not local_dir.is_dir():
        raise ShareSyncError(f"Input directory does not exist: {local_dir}")

    log.info("Building tar.gz from %s", local_dir)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for fpath in sorted(local_dir.rglob("*")):
            if fpath.is_file():
                arcname = fpath.relative_to(local_dir).as_posix()
                tf.add(str(fpath), arcname=arcname)
    size = buf.tell()
    buf.seek(0)
    log.info("Uploading %d bytes to %s", size, upload_url)

    resp = client.put(
        upload_url,
        content=buf.read(),
        headers={"Authorization": f"Bearer {token}"},
    )
    if resp.status_code not in (200, 201, 204):
        raise ShareSyncError(
            f"PUT {upload_url} returned HTTP {resp.status_code}: {resp.text[:300]}"
        )
    log.info("Upload complete")


def propfind(
    base_url: str,
    token: str,
    *,
    client: httpx.Client,
) -> list[ShareSyncEntry]:
    """PROPFIND Depth:1 on *base_url*. Returns all immediate children.

    The first entry in the list is the collection itself; the rest are its
    children. Callers typically skip is_collection=True entries.
    """
    resp = client.request(
        "PROPFIND",
        base_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Depth": "1",
        },
    )
    if resp.status_code not in (207, 200):
        raise ShareSyncError(
            f"PROPFIND {base_url} returned HTTP {resp.status_code}: {resp.text[:300]}"
        )
    return _parse_multistatus(resp.text)


def _parse_multistatus(xml_text: str) -> list[ShareSyncEntry]:
    """Parse a WebDAV multistatus XML body into ShareSyncEntry objects."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ShareSyncError(f"Could not parse PROPFIND XML: {exc}") from exc

    entries: list[ShareSyncEntry] = []
    for response_el in root.findall(_dav("response")):
        href_el = response_el.find(_dav("href"))
        href = (href_el.text or "").strip() if href_el is not None else ""
        # URL-decode for the display name, keep original href for URL construction
        decoded = unquote(href)
        name = decoded.rstrip("/").rsplit("/", 1)[-1]

        is_collection = False
        content_length: int | None = None
        for propstat in response_el.findall(_dav("propstat")):
            prop = propstat.find(_dav("prop"))
            if prop is None:
                continue
            rt = prop.find(_dav("resourcetype"))
            if rt is not None and rt.find(_dav("collection")) is not None:
                is_collection = True
            cl = prop.find(_dav("getcontentlength"))
            if cl is not None and cl.text:
                try:
                    content_length = int(cl.text)
                except ValueError:
                    pass

        entries.append(ShareSyncEntry(
            href=href,
            name=name,
            is_collection=is_collection,
            content_length=content_length,
        ))
    return entries


def download_file(
    file_url: str,
    token: str,
    local_path: Path,
    *,
    client: httpx.Client,
) -> None:
    """GET *file_url* and stream it to *local_path*."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with client.stream(
        "GET",
        file_url,
        headers={"Authorization": f"Bearer {token}"},
    ) as resp:
        if resp.status_code != 200:
            raise ShareSyncError(
                f"GET {file_url} returned HTTP {resp.status_code}"
            )
        with local_path.open("wb") as f:
            for chunk in resp.iter_bytes(chunk_size=65_536):
                f.write(chunk)
    log.debug("Downloaded %s -> %s", file_url, local_path)


def file_url_from_entry(base_url: str, entry: ShareSyncEntry) -> str:
    """Build the full download URL for a PROPFIND entry.

    Combines the scheme+host from *base_url* with the href path from the entry,
    since PROPFIND hrefs are absolute paths (e.g. /dav/spaces/xxx/file.txt).
    """
    parsed = urlparse(base_url)
    return f"{parsed.scheme}://{parsed.netloc}{entry.href}"
